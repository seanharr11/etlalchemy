# etl-alchemy
Extract, Transform and Load...Migrate any SQL Database with 4 Lines of Code.

# Basic Usage
```python
from etlalchemy import ETLAlchemy, ETLAlchemyMigrator

mssql_db_source = ETLAlchemySource("mssql+pyodbc://username:password@DSN_NAME")

mysql_db_target = ETLAlchemyTarget("mysql://username:password@hostname/db_name", drop_database=True)
mysql_db_target.addSource(mssql_db_source)
mysql_db_target.migrate()
````
