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

# Source -> Target Compatibility Matrix 
_(Sources in first column, destinations in first row)_

|     | MySQL | Postgresql | MSSQL | Oracle | SQLite |
| :--- | :--- | :--- | :--- | :---- | :--- |
| **MySQL** |![alt text][failure]|![alt text][success]|![alt text][success]|![alt text][success]|![alt text][success]|
| **Postgresql** |![alt text][failure]|![alt text][success]|![alt text][success]|![alt text][success]|![alt text][success]|
| **MSSQL** |![alt text][success]|![alt text][success]|![alt text][success]|![alt text][success]|![alt text][success]|
| **Oracle** |![alt text][success]|![alt text][success]|![alt text][success]|![alt text][success]|![alt text][success]|
| **SQLite** |![alt text][success]|![alt text][failure]|![alt text][failure]|![alt text][success]|![alt text][success]|

    

