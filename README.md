# etl-alchemy
Extract, Transform and Load...Migrate any SQL Database with 4 Lines of Code.

# Basic Usage
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget

mssql_db_source = ETLAlchemySource("mssql+pyodbc://username:password@DSN_NAME")

mysql_db_target = ETLAlchemyTarget("mysql://username:password@hostname/db_name", drop_database=True)
mysql_db_target.addSource(mssql_db_source)
mysql_db_target.migrate()
````

[success]: https://github.com/seanharr11/etl-alchemy/blob/performance/img/green_check.png "Success"
[failure]: https://github.com/seanharr11/etl-alchemy/blob/performance/img/red_x.png "Failure"

# Source -> Target Performance/Timing Matrix
_Sources in first column, destinations in first row (DB Size: 4 million rows, 150MB)_

|     | MySQL | Postgresql | MSSQL | Oracle | SQLite |
| :--- | :--- | :--- | :--- | :---- | :--- |
| **MySQL** |4m:38s|4m:31s|61m:27s|63m:16s|2m:18s|
| **Postgresql** |5m:9s|4m:24s|58m:9s|61m:29s|3m:11s|
| **MSSQL** |5m:58s|5m:26s|![alt text][failure]|60m:8s|5m:17s|
| **Oracle** |40m:14s|39m:25s|82m:26s|![alt text][failure]|![alt text][failure]|
| **SQLite** |4m:51s|4m:51s|67m:29s|![alt text][failure]|2m:11s|

# Examples

**Providing a list of tables to include/exclude in migration**
```python
from etl_alchemy import ETLAlchemySource, ETLAlchemyTarget

# Load ALL tables EXCEPT 'salaries'
source = ETLAlchemySource(conn_string="mysql://etlalchemy:etlalchemy@localhost/employees",\
                          excluded_tables=["salaries"])
# Conversely, you could load ONLY the 'salaries' table
"""source = ETLAlchemySource(conn_string="mysql://etlalchemy:etlalchemy@localhost/employees",\
                          included_tables=["salaries"])"""

target = ETLAlchemyTarget(conn_string="postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=True)
target.addSource(source)
target.migrate()
```
**Migrate Schema ONLY**
```python
from etl_alchemy import ETLAlchemySource, ETLAlchemyTarget

source = ETLAlchemySource(conn_string="mysql://etlalchemy:etlalchemy@localhost/employees")

target = ETLAlchemyTarget(conn_string="postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=True)
target.addSource(source)
# Note that each phase (schema, data, index, fk) is independent of all others, 
# and can be run standalone, or in any combination. (Obviously you need a schema to send data, etc...)
target.migrate(migrate_fks=False, migrate_indexes=False, migrate_data=False, migrate_schema=True)
```
**Skip columns and tables if they are empty**
```python
from etl_alchemy import ETLAlchemySource, ETLAlchemyTarget
# This will skip tables with no rows (or all empty rows), and ignore them during schema migration
# This will skip columns if they have all NULL values, and ignore them during schema migration
source = ETLAlchemySource(conn_string="mysql://etlalchemy:etlalchemy@localhost/employees",\
                          skip_column_if_empty=True,\
                          skip_table_if_empty=True)
target = ETLAlchemyTarget(conn_string="postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=True)
target.addSource(source)
target.migrate()
```
**Enable 'upserting' of data**
```python
from etl_alchemy import ETLAlchemySource, ETLAlchemyTarget

source = ETLAlchemySource(conn_string="mysql://etlalchemy:etlalchemy@localhost/employees")
# This will leave the target DB as is, and if the tables being migrated from Source -> Target
# already exist on the Target, then rows will be updated based on PKs if they exist, or 
# inserted if they DNE on the Target table.
target = ETLAlchemyTarget(conn_string="postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=False)
target.addSource(source)
target.migrate()
```

