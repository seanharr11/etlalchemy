from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import create_engine, MetaData
#import dill
import logging


class ETLAlchemyTarget():
    def __init__(self, conn_string, drop_database=False):
        self.drop_database = drop_database
        self.conn_string = conn_string
        ##########################
        ### Right now we only assume  sql database...
        ##########################
        self.sources = []
        self.logger = logging.getLogger("ETLAlchemyTarget")
        for h in list(self.logger.handlers):
            # Clean up any old loggers...(useful during testing w/ multiple log_files)
            self.logger.removeHandler(h)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s (%levelname)s) - %(message)s')
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    # Add an ETLAlchemySource to the list of 'sources'
    """ Each 'migrator' represents a source SQL DB """
    def addSource(self, source):
        if not getattr(source, 'migrate'):
            raise NotImplemented("Source '" + str(source) + "' has no function 'migrate'...")
        self.sources.append(source)
    def migrate(self, migrate_schema=True, migrate_data=True, migrate_fks=True, migrate_indexes=True):
        if self.drop_database == True:
            """ DROP THE DATABASE IF drop_database IS SET TO TRUE"""
            dst_engine = create_engine(self.conn_string)
            
            self.logger.info(dst_engine.dialect.name)
            #dst_engine.execute("select name from sys.sysdatabases where dbid=db_id()")
            ############################
            ### Hack for SQL Server using DSN's 
            ### and not havine DB name in connection_string
            ############################
            if dst_engine.dialect.name.upper() == "MSSQL":
                db_name = list(dst_engine.execute("SELECT DB_NAME()").fetchall())[0][0]
                self.logger.warning("Can't drop database {0} on MSSQL, dropping tables instead...".format(db_name))
                m = MetaData()
                m.bind = dst_engine
                m.reflect()
                m.drop_all()
            elif dst_engine.dialect.name.upper() == "ORACLE":
                db_name = list(dst_engine.execute("SELECT SYS_CONTEXT('userenv','db_name') FROM DUAL").fetchall())[0][0]
                self.logger.warning("Can't drop database {0} on ORACLE, dropping tables instead...".format(db_name))
                m = MetaData()
                m.bind = dst_engine
                m.reflect()
                m.drop_all()
            else:
                if dst_engine.url and database_exists(dst_engine.url):
                    self.logger.warning(dst_engine.url)
                    self.logger.warning("Dropping database '{0}'".format(self.conn_string.split("/")[-1]))
                    drop_database(dst_engine.url)
                    self.logger.info("Creating database '{0}'".format(self.conn_string.split("/")[-1]))
                    create_database(dst_engine.url)
                else:
                    self.logger.info("Database DNE...no need to nuke it.")
                    create_database(dst_engine.url)
        for source in self.sources:
            self.logger.info("Sending source '" + str(source) + "' to destination '" + str(self.conn_string) + "'")
            source.migrate(self.conn_string, migrate_schema=migrate_schema, migrate_data=migrate_data)
            if migrate_indexes:
                source.add_indexes(self.conn_string)
            if migrate_fks:
                if dst_engine.dialect.name.lower() == "mssql":
                    self.logger.warning("** SKIPPING 'Add Foreign Key Constraints' BECAUSE 'sqlalchemy_migrate' DOES NOT SUPPORT fk.create() ON *MSSQL*")
                else:
                    source.add_fks(self.conn_string)
            source.print_timings()

