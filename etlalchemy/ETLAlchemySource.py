from itertools import islice
import MySQLdb
from literal_value_generator import dump_to_sql_statement, dump_to_csv, dump_to_oracle_insert_statements
import random
from migrate.changeset.constraint import ForeignKeyConstraint
from datetime import datetime
import time
from copy import deepcopy
import pickle
import sqlalchemy
import logging
#from clean import cleaners
from sqlalchemy.sql import select
from sqlalchemy.schema import CreateTable, Column
from sqlalchemy.sql.schema import Table, Index
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, MetaData, func, and_
from sqlalchemy.engine import reflection
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.types import Numeric, BigInteger, Integer, DateTime, Date
import inspect as ins
import re
import csv
from schema_transformer import SchemaTransformer
import os

# Parse the connection_string to find relevant info for each db engine #

"""
 An instance of 'ETLAlchemySource' represents 1 DB. This DB can be sent to multiple
 'ETLAlchemyTargets' via calls to ETLAlchemySource.migrate(). See samples for info...
"""

class ETLAlchemySource():
   def __init__(self,\
                 conn_string,\
                 global_ignored_col_suffixes=['crtd_db_ind', 'transmit_ind'],\
                 global_renamed_col_suffixes={},\
                 column_schema_transformation_file=None,\
                 table_schema_transformation_file=None,\
                 included_tables=None,\
                 excluded_tables=None,\
                 skip_table_if_empty=False,\
                 skip_column_if_empty=False,\
                 dill_cleaner_file=None,\
                 dill_mapper_file=None,
                 log_file=None):
       self.logger = logging.getLogger("ETLAlchemySource")
       self.logger.propagate = False
       handler = logging.StreamHandler()
       if log_file != None:
           for h in list(self.logger.handlers):
               # Clean up any old loggers...(useful during testing w/ multiple log_files)
               self.logger.removeHandler(h)
           handler = logging.FileHandler(log_file)
       formatter = logging.Formatter('%(name)s (%(levelname)s) - %(message)s')
       handler.setFormatter(formatter)

       self.logger.addHandler(handler)
       self.logger.setLevel(logging.INFO)
       # Load the json dict of cleaners...
       # {'table': [cleaner1, cleaner2,...etc], 'table2': [cleaner1,...cleanerN]}
       
       self.included_tables = included_tables
       self.excluded_tables = excluded_tables

       # Set this to 'False' if you are using either of the following MSSQL Environments:
       # 1.) AWS SQL Server
       #   ---> The 'bulkadmin' role required for BULK INSERT permissions is not available in AWS (see https://forums.aws.amazon.com/thread.jspa?threadID=122351)
       # 2.) Azure SQL
       #   ---> The 'BULK INSERT' feature is disabled in the Microsoft Azure cloud
       # ** Otherwise, setting this to 'True' will vastly improve run-time...
       self.enable_mssql_bulk_insert = False

       self.current_ordered_table_columns = []
       if dill_cleaner_file:
           self.cleaners = dill.load(dill_cleaner_file)
       else:
           self.cleaners = {}
       
       self.schemaTransformer = SchemaTransformer(column_transform_file=column_schema_transformation_file,\
                                                  table_transform_file=table_schema_transformation_file,\
                                                  global_renamed_col_suffixes=global_renamed_col_suffixes)

       self.dst_engine = None   
       self.constraints = {}
       self.indexes = {}
       self.fks = {}
       self.engine = None
       self.connection = None
       self.orm = None
       self.database_url = conn_string

       self.columnCount = 0
       self.tableCount = 0
       self.emptyTableCount = 0
       self.emptyTables = []
       self.deletedTableCount = 0
       self.deletedColumnCount = 0
       self.deletedColumns = []
       self.nullColumnCount = 0
       self.nullColumns = []
       self.referentialIntegrityViolations = 0
       self.uniqueConstraintViolations = []
       self.uniqueConstraintViolationCount = 0

       self.skipColumnIfEmpty = skip_table_if_empty
       self.skipTableIfEmpty = skip_column_if_empty

       self.totalIndexes = 0
       self.indexCount = 0
       self.skippedIndexCount = 0
       
       self.totalFKs = 0
       self.fkCount = 0
       self.skippedFKCount = 0
       # Config
       self.checkReferentialIntegrity = False
       self.riv_arr = []
       self.start = datetime.now()
   
       self.global_ignored_col_suffixes = global_ignored_col_suffixes

       self.times = {} #Map Tables to Names...
   
   def standardizeColumnType(self, column, raw_rows):
       old_column_class = column.type.__class__    
       column_copy = Column(column.name,\
               column.type,\
               nullable=column.nullable,\
               unique=column.unique,
               primary_key=column.primary_key)
               #index=column.index)
       """"""""""""""""""""""""""""""""
       """ *** STANDARDIZATION *** """
       """"""""""""""""""""""""""""""""
       ##############################
       ### Duck-typing to remove 
       ### database-vendor specific column types
       ##############################
       base_classes = map(lambda c: c.__name__, column.type.__class__.__bases__)
       
       if "Enum" in base_classes:
           # Hack for error 'postgresql enum type requires a name'
           if self.dst_engine.dialect.name.lower() == "postgresql":
               self.logger.info(dir(column.type))
               self.logger.info(str(column))
               column_copy.type = column.type
               column_copy.type.__class__ = column.type.__class__.__bases__[0]
               # Name the enumeration 'table_column'
               column_copy.type.name = str(column).replace(".", "_")
           else:
               column_copy.type.__class__ = column.type.__class__.__bases__[0]
       elif "String" in base_classes:
           #########################################
           ### Get the VARCHAR size of the column...
           ######################################## 
           varchar_length = column.type.length
           column_copy.type.length = varchar_length
           ##################################
           ### Strip collation here ...
           ##################################
           column_copy.type.collation = None
           for row in raw_rows:
               data = row[self.current_ordered_table_columns.index(column.name)]
               if varchar_length and data and len(data) > varchar_length:
                   self.logger.critical("Length of column '{0}' exceeds VARCHAR({1})".format(column.name, str(varchar_length)))
               if data != None:
                   null = False
       elif "Date" in base_classes or "DateTime" in base_classes:
           ####################################
           ### Determine whether this is a Date
           ### or Datetime field
           ###################################
           typeCount = {}
           types = set([])
           for row in raw_rows:
               data = row[self.current_ordered_table_columns.index(column.name)]
               types.add(data.__class__.__name__)
               if typeCount.get(data.__class__.__name__):
                   typeCount[data.__class__.__name__] += 1
               else:
                   typeCount[data.__class__.__name__] = 1
               if data != None:
                   null = False
           self.logger.warning(str(typeCount))
           if typeCount.get("datetime"):
               column_copy.type = DateTime()
           else:
               column_copy.type = Date()

       elif "Numeric" in base_classes or "Float" in base_classes: 
           ####################################
           ### Check all cleaned_rows to determine 
           ### if column is decimal or integer
           ####################################
           mantissa_max_digits = 0
           left_hand_max_digits = 0
           mantissa_max_value = 0
           intCount = 0
           maxDigit = 0
           typeCount = {}
           types = set([])
           for row in raw_rows:
               data = row[self.current_ordered_table_columns.index(column.name)]
               types.add(data.__class__.__name__)
               if typeCount.get(data.__class__.__name__):
                   typeCount[data.__class__.__name__] += 1
               else:
                   typeCount[data.__class__.__name__] = 1
               ######################
               ### Check for NULL data (We will drop column if all rows contain null)
               ######################
               if data != None:
                   null = False
               if data.__class__.__name__ == 'Decimal' or data.__class__.__name__ == 'float':
                   splt = str(data).split(".")
                   if len(splt) == 1:
                       intCount += 1
                       continue
                   
                   left_hand_digits = splt[0]
                   mantissa_digits =  splt[1] 

                   # Store greatest mantissa to check for decimal cols that should be integers...(i.e. if m = 3.000)
                   mantissa_max_digits = len(mantissa_digits) if len(mantissa_digits) > mantissa_max_digits else mantissa_max_digits
                   left_hand_max_digits = len(left_hand_digits) if len(left_hand_digits) > left_hand_max_digits else left_hand_max_digits
                   mantissa_max_value = int(mantissa_digits) if int(mantissa_digits) > mantissa_max_value else mantissa_max_value
               elif data.__class__.__name__ == 'int':
                   intCount += 1
                   maxDigit = data if data > maxDigit else maxDigit
           self.logger.info(" --> " + str(column.name) + "..." + str(typeCount))
           if mantissa_max_value > 0:
               total_left_digits = max(len(str(maxDigit)), (mantissa_max_digits))
               column_copy.type = Numeric(total_left_digits + left_hand_max_digits, mantissa_max_digits)
               if intCount > 0:
                   self.logger.warning("Column '" +column.name + "' contains decimals and integers, resorting to type 'Numeric'") 
               if column.primary_key == True:
                   self.logger.warning("Column '" +column.name + "' is a primary key, but is of type 'Decimal'")
               if mantissa_max_value == 0:
                   self.logger.warning("Column '" + column.name + "' is of type 'Decimal', but contains no mantissas > 0 (i.e. 3.00, 2.00, etc..)\n    --Consider changing column type to 'Integer'")
           else:
               # 2^32 - 1
               if maxDigit > 4294967295:
                   column_copy.type = BigInteger()
               else:
                   column_copy.type = Integer()
       elif "TypeEngine" in base_classes:
           self.logger.warning("Type '{0}' has no base class!".format(column.type.__class__.__name__))
       elif "_Binary" in base_classes:
           self.logger.warning("Base types include '_Binary'")
       else: 
           #####################################################
           ### Column type is not dialect-specific, but...
           ### ... we need to check for null columns still b/c
           ### ... we default to True !
           ######################################################
           if hasattr(column.type, 'collation'):
               column_copy.type.collation = None
           self.logger.info("({0}) Class: ".format(column_copy.name)  + str(column.type.__class__.__name__))
           self.logger.info("({0}) ---> Bases: ".format(column_copy.name) + str(column.type.__class__.__bases__))
          
           column_copy.type.__class__ = column.type.__class__.__bases__[0]
       return column_copy
   
   def addOrEliminateColumn(self, T, T_dst_exists, column, column_copy, raw_rows):
       old_column_class = column.type.__class__  
       table_name = T.name
       null = True
       for row in raw_rows:
           data = row[self.current_ordered_table_columns.index(column.name)]
           if data != None:
               # There exists at least 1 row with a non-Null value for the column
               null = False
       cname = column_copy.name
       columnHasGloballyIgnoredSuffix = len(filter(lambda s: cname.find(s) > -1, self.global_ignored_col_suffixes)) > 0
       
       oldColumns = self.current_ordered_table_columns
       oldColumnsLength = len(self.current_ordered_table_columns)
       self.current_ordered_table_columns = self.schemaTransformer.transform_column(column_copy, T.name, self.current_ordered_table_columns)
       if oldColumnsLength != len(self.current_ordered_table_columns):
           # A column is scheduled to be deleted
           self.logger.warning(" ------> Column '" + cname + "' is scheduled to be deleted -- **NOT** migrating this column...")
           self.deletedColumnCount += 1
           self.deletedColumns.append(table_name + "." + cname)
           if T_dst_exists:
               pass
               #TODO: Delete the column from T_dst
           return False
       elif oldColumns != self.current_ordered_table_columns:
           # Column was renamed
           if T_dst_exists:
               pass
               #TODO Add the column to the table...
           else:
               T.append_column(column_copy) #column_copy has updated datatype...
           logging.info(" -----> '{0}' ({1}) => '{2}' ({3})".format(column.name, str(old_column_class), column_copy.name, str(column_copy.type.__class__)))
           return True
       elif null == True and self.skipColumnIfEmpty == True: 
           self.nullColumnCount += 1
           self.nullColumns.append(table_name + "." + column_copy.name)
           self.logger.warning("Column '" + table_name + "." + column_copy.name + "' has all NULL entries, skipping...")
           return False
           #TODO: When adding indexes and FKs, check to make sure the FK or index isn't on this column...
       else:
           if T_dst_exists:
               pass
               #TODO Add the column to the table...
           else:
               T.append_column(column_copy) #column_copy has updated datatype...
           logging.info(" ******* " + str(column.name) + " == " + str(old_column_class) + " => " + str(column_copy.type.__class__))
           return True

   def transformTable(self, T):
       ################################
       ### Run Table Transformations
       ################################
       """ This will update the table 'T' in-place (i.e. change the table's name) """
       if self.schemaTransformer.transform_table(T) == False:
           self.logger.info(" ---> Table ({0}) is scheduled to be deleted according to table transformations...".format(T.name))
           # Clean up FKs and Indexes on this table...
           del self.indexes[T.name]
           del self.fks[T.name]
           self.deletedTableCount += 1
           self.deletedColumns += map(lambda c: T.name + "." +  c.name, T.columns)
           self.deletedColumnCount += len(T.columns)
           return None
       return True

   def checkMultipleAutoincrementIssue(self, auto_inc_count, pk_count, T):     
       if auto_inc_count > 0 and pk_count > 1: 
           #and engine == MySQL.innoDB...
           self.logger.warning(" **** Table '" + T.name + "' contains a composite primary key, with an auto-increment attribute tagged on 1 of the columns.\n*************************************\n  **** We are dropping the auto-increment field ****\n.********************************\n (why? MySQL -> InnoDB Engine does not support this. Try MyISAM for support - understand that Oracle does not allow auto-increment fields, but uses sequences to create unique composite PKs")  
           for c in T.columns:
               if c.autoincrement and c.primary_key:
                   c.autoincrement = False
   def transformData(self, T_src, raw_rows):
        """"""""""""""""""""""""""""""
        """ *** TRANSFORMATION *** """
        """"""""""""""""""""""""""""""
        # Transform the data first
        #if self.cleaners.get(T_src.name):
            #TODO: Finish Implementing TableCleaner.clean(rows)
            #TC = TableCleaner(T_src)
            #TC.loadCleaners(self.cleaners[table_name])
            #TC.clean(raw_rows)
        # Transform the schema second (by updating the column names [keys of dict])
        self.schemaTransformer.transform_rows(raw_rows, self.current_ordered_table_columns, T_src.name)
       
        
   def createTable(self, T_dst_exists, T):
        if not T_dst_exists:    
            self.logger.info(" --> Creating table '{0}'".format(T.name))
            try:
                T.create()
                return True
            except Exception as e:
                self.logger.error("Failed to create table '{0}'\n\n{1}".format(T.name, e))
                return False
        else:
            self.logger.warning("Table '{0}' already exists - not creating table, reflecting to get new changes instead..".format(T.name))
            insp = inspect(self.dst_engine)
            insp.reflecttable(T, None)
            return True
            # We need to Upsert the data...

   def sendData(self, table, columns):
       Session = sessionmaker(bind=self.dst_engine)
       session = Session()
       data_file_path = os.getcwd() + "/" + table + ".sql"

       self.logger.info("Transferring data from local file '{0}' to target DB".format(table + ".sql"))
       if self.dst_engine.dialect.name.lower() == "mssql":
           username = self.dst_engine.url.username
           password = self.dst_engine.url.password
           dsn = self.dst_engine.url.host
           db_name = list(self.dst_engine.execute("SELECT DB_NAME()").fetchall())[0][0]
           if self.enable_mssql_bulk_insert == False:
               ######################################
               ### SQL Azure does not support BULK INSERT
               ### ... we resort to a Large INSERT statement
               ######################################
               self.logger.info("Sending data to target MSSQL instance...(Slow - enable_mssql_bulk_insert = False)")
               os.system("cat {4} | isql {0} {1} {2} -d{3} -v".format(dsn, username, password, db_name, data_file_path))    
               self.logger.info("Done.")
           else:
               try:
                   conn = session.connection()
                   t1 = conn.begin()
                   self.logger.info("Sending data to target MSSQL instance...(Fast [BULK INSERT])")

                   conn.execute("BULK INSERT {0} FROM '{1}' WITH ( \
                                     fieldterminator = '|,', \
                                     rowterminator = '\n' \
                                   );".format(data_file_path, table))
                   t1.commit()
               except sqlalchemy.exc.ProgrammingError as e:
                   self.logger.critical("BULK INSERT operation not supported on your target MSSQL server instance. [It is likely that you are running on Azure SQL (no bulk insert feature), or AWS SQL Server(no bulkadmin role)]. \n" + \
                                         "*****************************************************************************\n" + \
                                         "**** Re-run with 'self.enable_mssql_bulk_insert = False' **...but expect slow data transfer. ****\n" + \
                                         "*****************************************************************************\n" + \
                                         "\nOriginal Exception: " + str(e))
                   exit(1)        
               self.logger.info("Done.")
           #os.system("cat payload.sql | isql {0} {1} {2} -d{3} -v".format(dsn, username, password, db_name))    
       elif self.dst_engine.dialect.name.lower() == "mysql":
           username = self.dst_engine.url.username
           password = self.dst_engine.url.password
           db_name = self.dst_engine.url.database
           host = self.dst_engine.url.host
           self.logger.info("Sending data to target MySQL instance...(Fast [mysqlimport])")
           os.system("mysqlimport -v -h{0} -u{1} -p{2} \
                         --compress --local --fields-terminated-by=\",\" --fields-enclosed-by=\"'\" --fields-escaped-by='\\' \
                         --columns={3} --lines-terminated-by=\"\n\" \
                         {4} {5}".format(host, username, password, ",".join(columns), db_name, data_file_path))
           self.logger.info("Done.")
       elif self.dst_engine.dialect.name.lower() == "postgresql":
           #TODO: Take advantage of psql> COPY FROM <payload.sql> WITH DELIMITER AS ","
           username = self.dst_engine.url.username
           password = self.dst_engine.url.password
           db_name = self.dst_engine.url.database
           host = self.dst_engine.url.host
           import psycopg2
           conn = psycopg2.connect("host='{0}' port='5432' dbname='{1}' user='{2}' password='{3}'".format(host, db_name,username,password))
           cur = conn.cursor()
           # Legacy method (doesn't work if not superuser, and if file is LOCAL
           cmd = """COPY {0} ({1}) FROM '{2}' WITH CSV QUOTE '''' ESCAPE '\\' """.format(table, ",".join(columns), data_file_path, "'")
           self.logger.info("Sending data to target Postgresql instance...(Fast [COPY ... FROM ... WITH CSV]): \n ----> {0}".format(cmd))
           with open(data_file_path, 'r') as fp_psql:
               # Most use command below, which loads data_file from STDIN to work-around permissions issues...
               cur.copy_from(fp_psql, str(table), null="NULL", sep="|", columns=tuple(map(lambda c: str(c), columns)))
           conn.commit()
           conn.close()
           self.logger.info("Done.")

       elif self.dst_engine.dialect.name.lower() == "sqlite":
           db_name = self.dst_engine.url.database
           self.logger.info("Sending data to target sqlite instance...(Fast [.import])")
           os.system("echo \".separator '|'\n.nullvalue NULL\n.import {0} {1}\" | sqlite3 {2}".format(data_file_path, table, db_name))
           # ** Note values will be inserted as 'NULL' if they are NULL.
           """
           with open("{0}.sql".format(table), "r") as fp:
               for line in fp.readlines():
                   self.dst_engine.execute(line)
           """
           self.logger.info("Done.")
       elif self.dst_engine.dialect.name.lower() == "oracle":
           with open(data_file_path, "r") as fp_orcl:
               lines_inserted = 0
               while True:
                   next_n_lines = list(islice(fp_orcl, 1001))
                   lines_inserted += 1000
                   if not next_n_lines:
                       break
                   self.dst_engine.execute("\n".join(next_n_lines))
                   self.logger.info("Inserted '{0}' rows".format(str(lines_inserted)))
       else:
           raise Exception("Not Implemented!")
       # Cleanup...
       self.logger.info("Cleaning up '{0}'.sql".format(table))
       os.remove(data_file_path)
       self.logger.info("Done")

   """
      Dumps the data to a file called <table_name>.sql in the CWD.
      Depending on the DB Target, either a CSV will be generated
      for optimized BULK IMPORT, or an INSERT query will be generated
      if BULK INSERTING a CSV is not supported (i.e. SQL Azure)
   """
   def dumpData(self, T_dst_exists, T, raw_rows, pks, sessionMaker):
        t_start_load = datetime.now()
        conn = self.dst_engine.connect()
        s = sessionMaker(bind=conn)
        data_file_path = os.getcwd() + "/{0}.sql".format(T.name)
        
        if not T_dst_exists:
           # Table "T" DNE in the destination table prior to this entire 
           # migration process. We can naively INSERT all rows in the buffer
           with open(data_file_path, "a+") as fp:                   
               if self.enable_mssql_bulk_insert == False and self.dst_engine.dialect.name.lower() == "mssql":
                   dump_to_sql_statement(T.insert().values(map(lambda r: dict(zip(self.current_ordered_table_columns, r)), raw_rows)), fp, self.dst_engine, T.name)
               elif self.dst_engine.dialect.name.lower() == "oracle":
                   self.logger.warning("** BULK INSERT operation not supported by Oracle. Expect slow run-time.\nThis utilty should be run on the target host to descrease network latency for given this limitation...")
                   dump_to_oracle_insert_statements(fp, self.dst_engine, T.name, raw_rows, self.current_ordered_table_columns)
               else:
                   dump_to_csv(fp, T.name, self.current_ordered_table_columns, raw_rows, self.dst_engine.dialect)
        else:
           ########################################
           #### We need to upsert the data...prepare upsertDict...
           ########################################
           upsertDict = {}
           self.logger.info("Gathering unique columns for upsert.")
           if len(pks) == 0:
               s = "There is no primary key defined on table '{0}'! We are unable to Upsert into this table without identifying unique rows!"
               raise Exception(s)
           unique_columns = filter(lambda c: c.name.lower() in pks, T.columns)
           self.logger.info("Unique columns are '{0}'".format(str(unique_columns)))
           q = select(unique_columns)
           rows = conn.execute(q).fetchall()
           for r in rows:
               uid = ""
               for pk in pks:
                   uid += str(getattr(r, pk))
               upsertDict[uid] = True
           ################################
           ### Now upsert each row...
           ################################
           self.logger.info("Creating 'upsert' statements for '" + str(len(raw_rows)) + "' rows, and dumping to '" + str(T.name) + ".sql'.")
          
           init_len = len(raw_rows)
           for r in range(init_len-1, -1, -1):
               uid = ""
               row = raw_rows[r]
               for pk in pks:
                   uid += str(row[self.current_ordered_table_columns[pk]])
               if upsertDict.get(uid):
                   with open(data_file_path, "a+") as fp:
                       stmt = T.update()\
                               .where(and_(*tuple(map(lambda pk: T.columns[pk] == row[self.current_ordered_table_columns.index(pk)], pks))))\
                               .values(dict(zip(self.current_ordered_table_columns, row)))
                       dump_sql_statement(stmt, fp, self.dst_engine, T.name)
                   del raw_rows[r]
           #################################
           ### Insert the remaining rows...
           #################################
           self.logger.info("Creating 'insert' statements for (the remaining)  "+ str(len(raw_rows)) + " rows, and dumping to '" + str(T.name) + ".sql' (because they DNE in the table!).")
           insertionCount = (len(raw_rows) / 1000) + 1
           raw_row_len = len(raw_rows)
           if len(raw_rows) > 0:
               self.logger.info(" ({0}) -- Inserting remaining '{0}' rows.".format(str(raw_row_len)))
               with open(data_file_path, "a+") as fp:
                   dump_sql_statement(T.insert().values(raw_rows), fp, self.dst_engine, T.name)
        conn.close()
   ## TODO: Have a 'Create' option for each table... 
   def migrate(self, destination_database_url, migrate_data=True, migrate_schema=True):
       """"""""""""""""""""""""
       """ ** REFLECTION ** """
       """"""""""""""""""""""""
       buffer_size = 10000
       
       if self.database_url.split(":")[0] == "oracle+cx_oracle":
           self.engine = create_engine(self.database_url, arraysize=buffer_size)
       else:
           self.engine = create_engine(self.database_url)
       self.insp = reflection.Inspector.from_engine(self.engine)
       self.table_names = self.insp.get_table_names()
       
       self.dst_engine = create_engine(destination_database_url)
       dst_meta = MetaData()
       
       Session = sessionmaker(bind=self.dst_engine)
       
       src_session = sessionmaker(bind=self.engine)()

       dst_meta.bind = self.dst_engine

       TablesIterator = self.table_names #defaults to ALL tables
       
       if self.included_tables and self.excluded_tables:
           raise SyncException("Can't provide 'included_tables' and 'excluded_tables', choose 1...aborting...")

       if self.included_tables:
           TablesIterator = self.included_tables
       elif self.excluded_tables:
           TablesIterator = list(set(TablesIterator) - set(self.excluded_tables))


       for table_name in TablesIterator:

           #######################
           ### Time each table...
           #######################
           self.times[table_name] = {}
           t_start = datetime.now()
           self.tableCount += 1
           self.logger.info("Syncing Table Schema '" + table_name + "'...")
           pk_count = 0
           auto_inc_count = 0
           
           T_src = Table(table_name, MetaData())
           try: 
               self.insp.reflecttable(T_src, None)
           except NoSuchTableError as table:
               self.logger.error("Table '" + table + "' not found in DB: '"  + destination + "'.")
               continue #skip to next table...
           ###############################
           ### Gather indexes & FKs
           ###############################
           self.indexes[table_name] = self.insp.get_indexes(table_name)
           self.fks[table_name] = self.insp.get_foreign_keys(table_name)
           self.logger.info("Loaded indexes and FKs for table '{0}'".format(table_name))
           if migrate_schema == True:
               T = Table(table_name, dst_meta)
               ###############################
               ### Check if DST table exists...
               ###############################
               T_dst_exists = True
               insp = inspect(self.dst_engine)
               try:
                   insp.reflecttable(T, None)
               except sqlalchemy.exc.NoSuchTableError as e:
                   T_dst_exists = False
                   self.logger.warning("Table '" + T.name + "' does not exist in the dst database, we will create this later...")
           
               """"""""""""""""""""""""""
               """ *** EXTRACTION *** """
               """"""""""""""""""""""""""
               #########################################################
               ### Generate the mapping of 'column_name' -> 'list index'
               ########################################################
               cols = map(lambda c: c.name,  T_src.columns)
               self.current_ordered_table_columns = [None] * len(cols)
               for i in range(0, len(cols)):
                   self.current_ordered_table_columns[i] = cols[i]
               ###################################
               ### Grab raw rows for data type checking...
               ##################################
               self.logger.info("Building query to fetch all rows from {0}".format(T_src.name))
               cnt = src_session.query(T_src).count() #self.engine.execute(T_src.count())
               resultProxy = self.engine.execute(T_src.select())
               self.logger.info("Done. ({0} total rows)".format(str(cnt)))
               #raw_rows = []
               #raw_rows = [dict(zip(row.keys(), row)) for row in rows]
               j = 0
               self.logger.info("Loading all rows into memory...")
               rows = []
               
               for i in range(1, (cnt / buffer_size) + 1):
                   self.logger.info("Fetched {0} rows".format(str(i * buffer_size)))
                   rows += resultProxy.fetchmany(buffer_size)
               rows += resultProxy.fetchmany(cnt % buffer_size)
               # Don't rely on Python garbage collection...
               resultProxy.close()

               assert(cnt == len(rows))
               
               raw_rows = [row for row in rows]
               self.logger.info("Done")
               pks = []


               ## TODO: Use column/table mappers, would need to update foreign keys...
               for column in T_src.columns:
                   self.columnCount += 1
                   ##############################
                   ### Check for multiple primary 
                   ### keys & auto-increment
                   ##############################
                   if column.primary_key == True:
                       pks.append(column.name.lower())
                       pk_count += 1
                   if column.autoincrement == True:
                       auto_inc_count += 1
                   ##############################
                   ### Standardize Column Type
                   ##############################
                   column_copy = self.standardizeColumnType(column, raw_rows) 
                   """"""""""""""""""""""""""""""
                   """ *** ELIMINATION I *** """
                   """"""""""""""""""""""""""""""
                   self.addOrEliminateColumn(T, T_dst_exists, column, column_copy, raw_rows)
               if self.dst_engine.dialect.name.lower() == "mysql":
                   #######################################
                   #### Remove auto-inc on composite PK's
                   #######################################
                   self.checkMultipleAutoincrementIssue(auto_inc_count, pk_count, T)
               if self.transformTable(T) == None:
                   #Skip the table, it is scheduled to be deleted...
                   continue
               elif len(T.columns) == 0:
                   #TODO: Delete table from T_dst
                   self.logger.warning("Table '" + T.name + "' has all NULL columns, skipping...")
                   self.emptyTableCount += 1
                   self.emptyTables.append(T.name)
                   continue
               elif len(raw_rows) == 0 and self.skipTableIfEmpty == True:
                   self.logger.warning("Table '" + T.name + "' has 0 rows, skipping...")
                   self.emptyTableCount += 1
                   self.emptyTables.append(T.name)
                   continue
               else:
                   tableCreationSuccess = self.createTable(T_dst_exists, T)
                   if not tableCreationSuccess:
                       continue
               t_start_clean = datetime.now()
               t_start_load = datetime.now()
                   
               """"""""""""""""""""""""""""""
               """" *** INSERT ROWS *** """""
               """"""""""""""""""""""""""""""
               data_file_path = os.getcwd() + "/{0}.sql".format(T.name)
               if os.path.isfile(data_file_path):
                   os.remove(data_file_path)
               # Delete the old file if it esists (i.e. if a previous run went bad and didn't clean up...)

               dst_meta.reflect(self.dst_engine)
               insp = inspect(self.dst_engine)
               insp.reflecttable(T, None)
               if migrate_data == True:
                   self.logger.info("Transforming & Inserting "+ str(len(raw_rows)) + " rows into table '" + str(T.name) + "'.")
                   # Create buffers of "1000" rows
                   #TODO: Parameterize "1000" as 'buffer_size' (should be configurable)
                   insertionCount = (len(raw_rows) / 1000) + 1
                   raw_row_len = len(raw_rows)
                   
                   if len(raw_rows) > 0:
                       for i in range(0, insertionCount):
                           startRow = 0  #i * 1000
                           endRow = 1000 #(i+1) * 1000 
                           virtualStartRow = i * 1000
                           virtualEndRow = (i+1) * 1000
                           if virtualEndRow > raw_row_len:
                               virtualEndRow = raw_row_len
                               endRow = raw_row_len
                           self.logger.info(" ({0}) -- Transforming rows: ".format(T.name) + str(virtualStartRow) + \
                                    " -> " + str(virtualEndRow) + "...({0} Total)".format(str(raw_row_len)))
                           self.transformData(T_src, raw_rows[startRow:endRow])
                           self.logger.info(" ({0}) -- Dumping rows: ".format(T.name) + str(virtualStartRow) + \
                                    " -> " + str(virtualEndRow) + " to '{1}.sql'...({0} Total)".format(str(raw_row_len), T.name))
                             
                           self.dumpData(T_dst_exists, T, raw_rows[startRow:endRow], pks, Session)
                           del raw_rows[startRow:endRow]
                   ################################################################
                   ### Now *actually* load the data via fast-CLI utilities
                   ################################################################
                       self.sendData(T.name, self.current_ordered_table_columns) # From <table_name>.sql

               t_stop_load = datetime.now()
               
                   
               ###################################
               ### Calculate operation time... ###
               ###################################
               #self.times[table_name] = [t_start, t_start_clean, t_start_load, t_start_index, t_start_constraint, t_stop]
               
               extraction_dt = t_start_clean - t_start
               extraction_dt_str = str(extraction_dt.seconds / 60) + "m:" + str(extraction_dt.seconds % 60) + "s"

               transform_dt = t_start_load - t_start_clean
               transform_dt_str = str(transform_dt.seconds / 60) + "m:" + str(transform_dt.seconds % 60) + "s"

               load_dt = t_stop_load - t_start_load
               load_dt_str = str(load_dt.seconds / 60) + "m:" + str(load_dt.seconds % 60) + "s"
               

               self.times[table_name]['Extraction Time'] = extraction_dt_str
               self.times[table_name]['Transform Time'] = transform_dt_str
               self.times[table_name]['Load Time'] = load_dt_str
               # End first table loop...
           
   def add_indexes(self, destination_database_url):
       dst_meta = MetaData()
       dst_meta.reflect(bind=self.dst_engine)
       dst_meta.bind = self.dst_engine
       Session = sessionmaker(bind=self.dst_engine)
       """"""""""""""""""""
       """ *** INDEX *** """
       """"""""""""""""""""
       ############################
       ### Add Indexes (Some db's require indexed references...
       ############################
       idx_count = 0
       for table_name in self.indexes.keys():
           t_start_index = datetime.now()
           pre_transformed_table_name = table_name

           indexes = self.indexes.get(table_name)
           ####################################
           ### Check to see if table_name 
           ### has been transformed...
           ####################################
           table_transform = self.schemaTransformer.tableTransformations.get(table_name)
           column_transformer = self.schemaTransformer.columnTransformations.get(table_name)
           if table_transform and table_transform.newTable not in ["", None]:
               # Update the table_name
               table_name = table_transform.newTable
           this_idx_count = 0               
           self.logger.info("Creating indexes for '" + table_name +  "'...")
           for i in indexes:

               self.totalIndexes += 1
               session = Session()
               col = i['column_names']
               unique = i['unique']
               # Name the index something compatible across all databases 
               #(i.e. can't create Index w/ same name as column in Postgresql)
               name = "IDX_" + table_name + "__" + "_".join(col) + "__" + str(this_idx_count) 
               #number prefix guarentees uniqueness (i.e. if multiple idx's on one column)
               cols = ()
               continueFlag = False
               self.logger.info("Checking validity of data indexed by: '{0}' (column = '{1}' - table = '{2}')".format(name, str(col), table_name))
               for c in col:
                   #####################################
                   #### Check for Column Transformations...
                   #####################################
                   if column_transformer and column_transformer.get(c) and column_transformer[c].newColumn not in ["", None]:
                       c = column_transformer[c].newColumn
                   #####################################
                   ### Check to see if the table and colum nexist
                   #####################################
                   tableHolder = dst_meta.tables.get(table_name)
                   if tableHolder == None:
                       continueFlag = True
                       self.logger.warning("Skipping index '" + str(name) + "' on column '" + table_name + "." + c + "' because the table does not exist in the destination DB schema.")
                   else:
                       columnHolder = dst_meta.tables.get(table_name).columns.get(c)
                       if str(columnHolder) == 'None':
                           self.logger.warning("Skipping index '" + str(name) + "' on column '" + table_name + "." + c + "' because the column does not exist in the destination DB schema.")
                           continueFlag = True #Skip to the next table...
                       cols += (dst_meta.tables.get(table_name).columns.get(c),)
               if continueFlag == True:
                   self.skippedIndexCount += 1
                   continue #Don't create this Index - the table/column don't exist!
               
               I = Index(name, *cols, unique=unique)
                       
               violationCount = 0
               if unique == True:
                   ############################################
                   ### Check for Unique Constraint Violations
                   ############################################
                   cols_tuple = tuple(cols)
                   if len(cols_tuple) > 1: #We have a composite index, let's deal with it...
                       uniqueGroups = session.query(*cols_tuple).group_by(*cols_tuple).count()
                       totalEntries = session.query(*cols_tuple).count()
                       violationCount = totalEntries - uniqueGroups #The difference represents repeated combinations of 'cols_tuple'
                   else: 
                       violationCount = session.query(*cols_tuple).group_by(*cols_tuple).\
                           having(func.count(*cols_tuple) > 1).count()
               if violationCount > 0:
                   self.logger.error("Duplicates found in column '" + str(col) + "' for unique index '" + name)
                   self.uniqueConstraintViolations.append(name + " (" + str(col) + ")")
                   self.uniqueConstraintViolationCount += violationCount
                   self.skippedIndexCount += 1
                   # TODO: Gather bad rows...
               else:   
                   self.logger.info("Adding Index: " + str(I))
                   session.close()
                   try:
                       I.create(self.dst_engine)
                   except sqlalchemy.exc.OperationalError as e:
                       self.logger.warning(str(e) + " -- it is likely that the Index already exists...")
                       self.skippedIndexCount += 1
                       continue
                   idx_count += 1
                   this_idx_count += 1
           self.logger.info("Done. (Added '" + str(this_idx_count) + "' indexes to '" + table_name + "'.)")
           
           t_stop_index = datetime.now()
           index_dt = t_stop_index - t_start_index
           self.times[pre_transformed_table_name]['Indexing Time'] = str(index_dt.seconds / 60) + "m:" + str(index_dt.seconds % 60) + "s"
       
       self.indexCount = idx_count
           
   def add_fks(self, destination_database_url):
       ############################
       ### Add FKs 
       ############################
       dst_meta = MetaData()
       if self.dst_engine.dialect.name.lower() == "mssql":
           raise Exception(\
                   "Adding Constraints to MSSQL is not supported by sqlalchemy_migrate...")
       dst_meta.reflect(bind=self.dst_engine)
       dst_meta.bind = self.dst_engine 
       Session = sessionmaker(bind=self.dst_engine)
       ##########################
       ### HERE BE HACKS!!!!
       ##########################
       """ 
          Problem: often times when porting DBs, data is old, not properly constrained
          and overall messy. FK constraints get violated without DBAs knowing it (in 
          engines that don't enforce or support FK constraints...

          Hack: Turn off FK checks when porting FKs..

          Better Solution: ...would be to insert data AFTER fks are inserted, row by
          row, and ask the user to correct the row in question, or delete it, this
          is more of a 'clean' operation than a 'sync'...

          Assumptions: We only support MySQL for this right now, PostgreSQL will be
          next - it is easy to turn off FK checks on a per-table basis in psql...

          TODO: Support PostgreSQL...

       """
       
       """""""""""""""""""""
       "" ** CONSTRAIN ** ""
       """""""""""""""""""""
       if self.dst_engine.dialect.name.upper() == "MYSQL":
           self.dst_engine.execute("SET foreign_key_checks = 0")
       elif self.dst_engine.dialect.name.upper() == "POSTGRESQL":
           self.logger.warning("Can't disable foreign key checks on POSTGRSQL")
       else:
           self.logger.warning("Can't disable foreign key checks...")
       
       inspector = inspect(self.dst_engine)
       for table_name in self.fks.keys():
           pre_transformed_table_name = table_name
           t_start_constraint = datetime.now()
           fks = self.fks[table_name]
           ####################################
           ### Check to see if table_name 
           ### has been transformed...
           ####################################
           table_transform = self.schemaTransformer.tableTransformations.get(table_name)
           if table_transform and table_transform.newTable not in ["", None]:
               # Update the table_name
               table_name = table_transform.newTable
           self.logger.info("Adding FKs to table '{0}' (previously {1})".format(table_name, pre_transformed_table_name)) 
           ########################
           ### Check that constrained table 
           ### exists in destiation DB schema
           ########################
           
           T = Table(table_name, dst_meta)
           try:
               inspector.reflecttable(T, None)
           except sqlalchemy.exc.NoSuchTableError as e:
               self.logger.warning("Skipping FK constraints on table '" + str(table_name) + "' because the constrained table does not exist in the destination DB schema.")
               self.skippedFKCount += len(self.fks[table_name])
               self.totalFKs += len(self.fks[table_name])
               continue #on to the next table...
           
           for fk in fks:
               cons_column_transformer = self.schemaTransformer.columnTransformations.get(pre_transformed_table_name)
               self.totalFKs += 1
               session = Session()
               #####################################
               #### Check for Column Transformations...
               #####################################
               constrained_columns = []
               for c in fk['constrained_columns']:
                   if cons_column_transformer and cons_column_transformer.get(c) and cons_column_transformer[c].newColumn not in ["", None]:
                       c = cons_column_transformer[c].newColumn
                   constrained_columns.append(c)
               constrained_cols = map(lambda x: T.columns.get(x), constrained_columns)
               ################################
               ### Check that the constrained columns
               ### exists in the destiation db schema
               ################################
               if len(constrained_cols) < len(fk['constrained_columns']):
                   self.logger.warning("Skipping FK constraint '" + constraint_name + "' because constrained columns '" + str(fk['constrained_columns']) + "' on table '" + str(table_name) + "' don't exist in the destination DB schema.")
                   session.close()
                   self.skippedFKCount += 1
                   continue
               ref_table = fk['referred_table']
               
               ####################################
               ### Check to see if table_name 
               ### has been transformed...
               ####################################
               table_transform = self.schemaTransformer.tableTransformations.get(ref_table)
               ref_column_transformer = self.schemaTransformer.columnTransformations.get(ref_table)
               if table_transform and table_transform.newTable not in ["", None]:
                   # Update the table_name
                   ref_table = table_transform.newTable
               T_ref = Table(ref_table, dst_meta)
               ############################
               ### Check that referenced table
               ### exists in destination DB schema
               ############################
               constraint_name = "FK__{0}__{1}".format(table_name.upper(), T_ref.name.upper())
               try:
                   inspector.reflecttable(T_ref, None)
               except sqlalchemy.exc.NoSuchTableError as e:
                   self.logger.warning("Skipping FK constraint '" + constraint_name + "' because referenced table '" + ref_table + "' doesn't exist in the destination DB schema. (FK Dependency not met)")
                   session.close()
                   self.skippedFKCount += 1
                   continue 
               ############################
               ## Check that referenced columns 
               ## Exist in destination DB schema
               ############################
               ref_columns = []
               for c in fk['referred_columns']:
                   if ref_column_transformer and ref_column_transformer.get(c) and ref_column_transformer[c].newColumns not in ["", None]:
                       c = ref_column_transformer[c].newColumn
                   ref_columns.append(c)
               referred_columns = map(lambda x: T_ref.columns.get(x), ref_columns)
               self.logger.info("Ref Columns: " + str(ref_columns))
               if len(referred_columns) < len(fk['referred_columns']):
                   self.logger.warning("Skipping FK constraint '" + constraint_name + "' because referenced columns '" + str(fk['referred_columns']) + "' on table '" + str(ref_table) + "' don't exist in the destination DB schema.")
                   session.close()
                   self.skippedFKCount += 1
                   continue
               
               

               ##################################
               ### Check for referential integrity violations
               ##################################                   
               if self.checkReferentialIntegrity == True:
                  if self.dst_engine.dialect.name.upper() in ["MYSQL", "POSTGRESQL"]: # HACKS
                      self.logger.info("Checking referential integrity of '" + str(table_name) + "." + str(constrained_columns) + " -> '" + str(T_ref.name) + "." + str(ref_columns) + "'")
                      t = session.query(T_ref.columns.get(referred_columns[0].name))
                      query2 = session.query(T)

                      q = query2.filter(and_(~T.columns.get(constrained_cols[0].name).in_(t), T.columns.get(constrained_cols[0].name) != None ))
                      bad_rows = session.execute(q).fetchall()
                     
                      if len(bad_rows) > 0:
                          self.logger.warning("FK from '" + T.name + "." + constrained_cols[0].name + " -> " + T_ref.name + "." + referred_columns[0].name + " was violated '" + str(len(bad_rows)) + "' times.")
                          self.referentialIntegrityViolations += len(bad_rows)
                          for row in bad_rows:
                              self.riv_arr.append(str(row.values())) 
                  
                  else:
                     self.logger.warning("Adding constraints only supported/tested for MySQL")
                     #raise NotImplemented("Referential Integrity Check is only tested for MySQL") 
               self.logger.info("Adding FK '" + constraint_name + "' to '" + table_name + "'")
               session.close() 
               cons = ForeignKeyConstraint(name=constraint_name,columns=constrained_cols, refcolumns=referred_columns, table=T)
               # Loop to handle tables that reference other tables w/ multiple columns & FKs
               creation_succesful = False
               max_fks = 15
               cnt = 0
               while not creation_succesful:
                   try:
                       cons.create(self.dst_engine)
                       creation_succesful = True
                   except sqlalchemy.exc.OperationalError as e: #MySQL Exception
                       self.logger.warning(str(e) + "\n ---> an FK on this table already references the ref_table...appending '{0}' to FK's name and trying again...".format(str(cnt)))
                       cons = ForeignKeyConstraint(name=constraint_name + "_{0}".format(str(cnt)),columns=constrained_cols, refcolumns=referred_columns, table=T)
                       cnt += 1
                       if cnt == max_fks:
                           self.logger.error("FK creation was unsuccesful (surpassed max number of FKs on 1 table which all reference another table)")
                           self.skippedFKCount += 1
                           break
                   except sqlalchemy.exc.ProgrammingError as e: # PostgreSQL Exception
                       self.logger.warning(str(e) + "\n ---> an FK on this table already references the ref_table...appending '{0}' to FK's name and trying again...".format(str(cnt)))
                       cons = ForeignKeyConstraint(name=constraint_name + "_{0}".format(str(cnt)),columns=constrained_cols, refcolumns=referred_columns, table=T)
                       cnt += 1
                       if cnt == max_fks:
                           self.logger.error("FK creation was unsuccesful (surpassed max number of FKs on 1 table which all reference another table)")
                           self.skippedFKCount += 1
                           break

               self.fkCount += 1
           t_stop_constraint = datetime.now()
           constraint_dt = t_stop_constraint - t_start_constraint
           constraint_dt_str = str(constraint_dt.seconds / 60) + "m:" + str(constraint_dt.seconds % 60) + "s"

           self.times[pre_transformed_table_name]['Constraint Time'] = constraint_dt_str
               
   def print_timings(self):    
       stop = datetime.now()
       dt = stop - self.start
       timeString = ""
       #if dt.seconds > 3600:
       #    timeString += (str(int(dt.seconds / 3600)) + ":")
       timeString += str(dt.seconds / 60) + "m:" + str(dt.seconds % 60) + "s"
       self.logger.info("""
       ========================
       === * Sync Summary * ===
       ========================\n
       Total Tables:                     {0}
       -- Empty Tables   (skipped)       {1}
       -- Deleted Tables (skipped)       {15}
       -- Synced Tables                  {2}\n
       ========================\n
       Total Columns:                    {3}
       -- Empty Columns   (skipped)      {4}
       -- Deleted Columns (skipped)      {16}
       -- Synced Columns                 {5}\n
       ========================\n
       Total Indexes                     {8}
       -- Skipped Indexes                {11}
       -- Synced Indexes                 {12}\n
       ========================\n
       Total FKs                         {9}
       -- Skipped FKs                    {13}
       -- Synced FKs                     {14}\n
       ========================\n
       Referential Integrity Violations: {6}
       ========================\n
       Unique Constraint Violations:     {10}
       ========================\n
       Total Time:                       {7}\n\n""".format(str(self.tableCount), str(self.emptyTableCount), str(self.tableCount-self.emptyTableCount),str(self.columnCount),str(self.nullColumnCount),str(self.columnCount-self.nullColumnCount),str(self.referentialIntegrityViolations), timeString, str(self.totalIndexes), str(self.totalFKs), str(self.uniqueConstraintViolationCount), str(self.skippedIndexCount), str(self.indexCount), str(self.skippedFKCount), str(self.fkCount), str(self.deletedTableCount), str(self.deletedColumnCount))) 
       #self.logger.warning("Referential Integrity Violations: \n" + "\n".join(self.riv_arr))
       self.logger.warning("Unique Constraint Violations: " + "\n".join(self.uniqueConstraintViolations))

   
   
       self.logger.info("""
       =========================
       === ** TIMING INFO ** ===
       =========================
                _____
             _.'_____`._
           .'.-'  12 `-.`.
          /,' 11      1 `.\/
         // 10      /   2 \|
        ;;         /       ::
        || 9  ----O      3 ||
        ::                 ;;
         \\ 8           4 //
          \`. 7       5 ,'/
           '.`-.__6__.-'.'
            ((-._____.-))
            _))       ((_
           '--'       '--'
       __________________________
       """)
       for (table_name, timings) in self.times.iteritems():
           self.logger.info(table_name)
           for key in timings.keys():
               self.logger.info("-- " + str(key) + ": " + str(timings[key]))
           self.logger.info("_________________________")
   
       self.schemaTransformer.failed_transformations = list(self.schemaTransformer.failed_transformations)
       if len(self.schemaTransformer.failed_transformations) > 0:
           self.logger.critical("\n".join(self.schemaTransformer.failed_transformations))
           self.logger.critical("""
           !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
           !!!! * '{0}' Old Columns had failed transformations !!!!
           !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
           """.format(str(len(self.schemaTransformer.failed_transformations))))
           
           self.logger.critical("\n".join(self.schemaTransformer.failed_transformations))
   
       ###########################################
       ### Write 'Deleted' columns out to a file...
       ###########################################
       removedColumns = self.deletedColumns + self.nullColumns
       with open("deletedColumns.csv", "w") as fp:
           fp.write("\n".join(map(lambda c: c.replace(".", ","), removedColumns)))
   

