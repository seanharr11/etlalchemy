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
_(Sources in first column, destinations in first row)_

|     | MySQL | Postgresql | MSSQL | Oracle | SQLite |
| :--- | :--- | :--- | :--- | :---- | :--- |
| **MySQL** |4m:38s|4m:31s|61m:27s|63m:16s|2m:18s|
| **Postgresql** |5m:9s|4m:24s|58m:9s|61m:29s|3m:11s|
| **MSSQL** |5m:58s|5m:26s|![alt text][failure]|![alt text][failure]|5m:17s|
| **Oracle** |40m:14s|39m:25s|82m:26s|![alt text][failure]|![alt text][failure]|
| **SQLite** |4m:51s|4m:51s|67m:29s|![alt text][failure]|2m:11s|

    

