from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import create_engine
import dill
import logging


class ETLAlchemy():
    def __init__(self, destination, drop_database=False):
        self.drop_database = drop_database
        self.destination = destination
        ##########################
        ### Right now we only assume  sql database...
        ##########################
        self.migrators = []
        self.logger = logging.getLogger("ETLAlchemy")
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s (%levelname)s) - %(message)s')
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    # Add an ETLAlchemyMigrator to the list of 'migrators'
    """ Each 'migrator' represents a source SQL DB """
    def addMigrator(self, migrator):
        if not getattr(migrator, 'migrate'):
            raise NotImplemented("Source '" + str(migrator) + "' has no function 'migrate'...")
        self.migrators.append(migrator)
    def migrate(self, database_name=None):
        if self.drop_database == True:
            """ DROP THE DATABASE IF drop_database IS SET TO TRUE"""
            dst_engine = create_engine(self.destination)
            
            self.logger.info(dir(dst_engine))
            self.logger.info(dir(dst_engine.driver))
            self.logger.info(dst_engine.driver)
            #dst_engine.execute("select name from sys.sysdatabases where dbid=db_id()")
            ############################
            ### Hack for SQL Server using DSN's 
            ### and not havine DB name in connection_string
            ############################
            if dst_engine.dialect.name.upper() == "MSSQL":
                db_name = list(dst_engine.execute("SELECT DB_NAME()").fetchall())[0][0]
                self.logger.warning("Can't drop database {0} on MSSQL...".format(db_name))
            elif dst_engine.dialect.name.upper() == "ORACLE":
                db_name = list(dst_engine.execute("SELECT SYS_CONTEXT('userenv','db_name') FROM DUAL").fetchall())[0][0]
                self.logger.warning("Can't drop database {0} on ORACLE...".format(db_name))
            else:
                if dst_engine.url and database_exists(dst_engine.url):
                    self.logger.warning(dst_engine.url)
                    self.logger.warning("Dropping database '{0}'".format(self.destination.split("/")[-1]))
                    drop_database(dst_engine.url)
                    self.logger.info("Creating database '{0}'".format(self.destination.split("/")[-1]))
                    create_database(dst_engine.url)
                else:
                    self.logger.info("Database DNE...no need to nuke it.")
                    create_database(dst_engine.url)
        for migrator in self.migrators:
            self.logger.info("Syncing migrator '" + str(migrator) + "' to destination '" + str(self.destination) + "'")
            migrator.migrate(self.destination, load_schema=True, load_data=True)
            migrator.add_indexes(self.destination)
            migrator.add_fks(self.destination)
            migrator.print_timings()

