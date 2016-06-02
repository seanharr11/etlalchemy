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
migrator = ETLAlchemyMigrator("mysql://seanh:NE16Pats@10.110.0.241/storm",\
        global_ignored_col_suffixes=['crtd_db_ind', 'transmit_ind'],\
        global_renamed_col_suffixes={'crtd_dt': 'created_at', 'updt_dt': 'updated_at', 'created': 'created_by', 'updated': 'updated_by'},\
        included_tables=['schools', 'scouts'],\
        excluded_tables=[],\
        column_schema_transformation_file="./transformations/column_mappings.csv",
        table_schema_transformation_file="./transformations/table_mappings.csv",
        dill_cleaner_file=None,\
        dill_mapper_file=None,\
        )
#dest = "sqlite:///foo.db"
dest = "mysql://root:Pats15Ball@localhost/test"

ETLa = ETLAlchemy(dest, drop_database=True)
ETLa.addMigrator(migrator)
ETLa.migrate()

