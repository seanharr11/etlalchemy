from etlalchemy import ETLAlchemySource, ETLAlchemyTarget


#logging.getLogger("sqlalchemy-magic").setLevel(logging.INFO)
###############################
### Migrate Entire Schema from Oracle -> MySQL, and ALL tables...
###############################
#dbc = db_converter("oracle+cx_oracle://scorpion_ne:scorpion_ne@10.110.0.216/PROD", included_tables=['asa$col_player', 'asa$col_cstat_defense'], excluded_tables=None, dill_cleaner_file=None, dill_mapper_file=None)
dbc = ETLAlchemySource("oracle+cx_oracle://scorpion_ne:scorpion_ne@10.110.0.216/PROD",\
        global_ignored_col_suffixes=['crtd_db_ind', 'crtd_db_id', 'transmit_ind', 'transmit_id'],\
        global_renamed_col_suffixes={'crtd_dt': 'created_at', 'updt_dt': 'updated_at', 'created': 'created_by', 'updated': 'updated_by'},\
        included_tables=[],\
        excluded_tables=[],\
        skip_column_if_empty=True,
        skip_table_if_empty=True
        
        )
#dbc = db_converter("mssql+pyodbc://sharrington:Boston2015@mssql_mc", included_tables=[], excluded_tables=None, dill_cleaner_file=None, dill_mapper_file=None)
#ETLa = ETLAlchemy("mysql://pats:pats@10.110.0.224/mssqlmc", drop_database=True)
#dest = "postgresql://seanh:NE16Pats@10.110.0.241/new_test"
dest = "mysql://pats:pats@localhost/test123"

ETLa = ETLAlchemyTarget(dest, drop_database=False)
ETLa.addSource(dbc)
#ETLa.migrate()
ETLa.migrate(migrate_data=False, migrate_schema=False)

