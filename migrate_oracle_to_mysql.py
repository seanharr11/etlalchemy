#! env/bin/python
from ETLAlchemyMigrator import ETLAlchemyMigrator
from ETLAlchemy import ETLAlchemy
import logging

logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

###############################
### Migrate Entire Schema from Oracle -> MySQL, and ALL tables...
###############################
#migrator = ETLAlchemyMigrator("oracle+cx_oracle://scorpion_ne:scorpion_ne@10.110.0.216/PROD",\
migrator = ETLAlchemyMigrator("mysql://root:Pats15Ball@localhost/employees",\
        global_ignored_col_suffixes=['crtd_db_ind', 'transmit_ind'],\
        global_renamed_col_suffixes={'crtd_dt': 'created_at', 'updt_dt': 'updated_at', 'created': 'created_by', 'updated': 'updated_by'},\
        included_tables=[],\
        excluded_tables=[],\
        column_schema_transformation_file="./transformations/column_mappings.csv",
        table_schema_transformation_file="./transformations/table_mappings.csv",
        dill_cleaner_file=None,\
        dill_mapper_file=None,\
        )
#dest = "sqlite:///foo.db"
dest = "oracle+cx_oracle://seanmon11:carousel13@etlalchemyoracle.cilwasbzice0.us-east-1.rds.amazonaws.com:1521/ORCL"
#dest = "mysql://seanmon11:carousel13@etlalchemymysql.cilwasbzice0.us-east-1.rds.amazonaws.com/testing"
#dest = "postgresql://seanmon11:carousel13@etlalchemy.cilwasbzice0.us-east-1.rds.amazonaws.com/testing"
#dest = "postgresql://SeanH:Pats15Ball@localhost/test"
#dest = "mssql+pyodbc://seanmon11:CAR0usel134182@AZUREMSSQL"

#dest = "mssql+pyodbc://seanmon11:CAR0usel134182@191.238.6.43/test"

ETLa = ETLAlchemy(dest, drop_database=True)
ETLa.addMigrator(migrator)
ETLa.migrate()

