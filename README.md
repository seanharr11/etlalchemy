# etlalchemy
Extract, Transform and Load...Migrate any SQL Database in 4 Lines of Code. *[Read more here...](http://thelaziestprogrammer.com/sharrington/databases/migrating-between-databases-with-etlalchemy)*

[![Donate](https://img.shields.io/badge/donate-paypal-blue.svg)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=DH544PY7RFSLA)
[![Donate](https://img.shields.io/badge/donate-gratipay-green.svg)](https://gratipay.com/etlalchemy/)
# Installation

```bash
pip install etlalchemy
# On El Capitan:
### pip install --ignore-installed etlalchemy
```

# Basic Usage
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget

source = ETLAlchemySource("mssql+pyodbc://username:password@DSN_NAME")
target = ETLAlchemyTarget("mysql://username:password@hostname/db_name", drop_database=True)
target.addSource(source)
target.migrate()
````

# Examples

**Provide a list of tables to include/exclude in migration**
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget

# Load ONLY the 'salaries' table
source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees",
                          included_tables=["salaries"])
# Conversely, you could load ALL tables EXCEPT 'salaries'
# source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees",\
#                          excluded_tables=["salaries"])

target = ETLAlchemyTarget("postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=True)
target.addSource(source)
target.migrate()
```
**Only migrate schema, or only Data, or only FKs, or only Indexes (or any combination of the 4!)**
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget

source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees")

target = ETLAlchemyTarget("postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=True)
target.addSource(source)
# Note that each phase (schema, data, index, fk) is independent of all others,
# and can be run standalone, or in any combination. (Obviously you need a schema to send data, etc...)
target.migrate(migrate_fks=False, migrate_indexes=False, migrate_data=False, migrate_schema=True)
```
**Skip columns and tables if they are empty**
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget
# This will skip tables with no rows (or all empty rows), and ignore them during schema migration
# This will skip columns if they have all NULL values, and ignore them during schema migration
source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees",\
                          skip_column_if_empty=True,\
                          skip_table_if_empty=True)
target = ETLAlchemyTarget("postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=True)
target.addSource(source)
target.migrate()
```
**Enable 'upserting' of data**
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget

source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees")
# This will leave the target DB as is, and if the tables being migrated from Source -> Target
# already exist on the Target, then rows will be updated based on PKs if they exist, or 
# inserted if they DNE on the Target table.
target = ETLAlchemyTarget("postgresql://etlalchemy:etlalchemy@localhost/test", drop_database=False)
target.addSource(source)
target.migrate()
```
**Alter schema (change column names, column types, table names, and Drop tables/columns)**
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget
# See below for the simple structure of the .csv's for schema changes
source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees",\
                          column_schema_transformation_file=os.getcwd() + "/transformations/column_mappings.csv",\
                          table_schema_transformation_file=os.getcwd() + "/transformations/table_mappings.csv")
target = ETLAlchemyTarget("postgresql://SeanH:Pats15Ball@localhost/test", drop_database=True)
target.addSource(source)
target.migrate()
```
| *column_mappings.csv* | *table_mappings.csv* |
| :--- | :--- |
|Column Name,Table Name,New Column Name,New Column Type,Delete|Table Name,New Table Name,Delete|
|last_name,employees,,,True|table_to_rename,new_table_name,False|
|fired,employees,,Boolean,False|table_to_delete,,True|
|birth_date,employees,dob,,False|departments,dept,False|

**Rename any column which ends in a given 'suffix' (or skip the column during migration)**
```python
from etlalchemy import ETLAlchemySource, ETLAlchemyTarget
# global_renamed_col_suffixes is useful to standardize column names across tables (like the date example below)
source = ETLAlchemySource("mysql://etlalchemy:etlalchemy@localhost/employees",\
                          global_ignored_col_suffixes=['drop_all_columns_that_end_in_this'],\
                          global_renamed_col_suffixes={'date': 'dt'},\ #i.e. "created_date -> created_dt"
                         )
target = ETLAlchemyTarget("postgresql://SeanH:Pats15Ball@localhost/test", drop_database=True)
target.addSource(source)
target.migrate()
```

# Known Limitations
1. 'sqlalchemy_migrate' does not support MSSQL FK migrations.
   *_(So, FK migrations will be skipped when Target is MSSQL)_
2. Currently not compatible with Windows
   * Several "os.system()" calls with UNIX-specific utilities
   * One option for Windows users is installing through the [Windows Subsystem for Linux (WSL)](https://msdn.microsoft.com/en-us/commandline/wsl/install_guide)
3. If Target DB is in the Azure Cloud (MSSQL), FreeTDS has some compatibility issues which are performance related. This may be noticed when migrating tables with 1,000,000+ rows into a Azure MSSQL Server.
4. Though the MSSQL 'BULK INSERT' feature is supported in this tool, it is NOT supported on either Azure environments, or AWS MSSQL Server environments (no 'bulkadmin' role allowed). Feel free to test this out on a different MSSQL environment!
5. Regression tests have not **(yet)** been created due to the unique **(and expensive)** way one must test all of the different database types.
6. Migrations *to* MSSQL and Oracle are extremely slow due to the lack of 'fast' import capabilities. 
  * 'SQL Loader' can be used on Oracle, and the 'BULK INSERT' operation can be used on MSSQL, however the former is a PITA to install, and the latter is not supported in several MSSQL environments (see 'Known Limitations' below).
  * 'BULK INSERT' *is supported* in etlalchemy (with limited testing), but "SQL LOADER" is not (yet).
7. When sending data to PostgreSQL, if the data contains VARCHAR() or TEXT() columns with carriage returns ('^M' or '\r'), these will be stripped.
  * This is due to the lack of the "ENCLOSED BY" option of psycopg.copy_from() - these chars are interpreted as literals, and in turn tell the COPY FROM operation that "the row ends here"

# Assumptions Made
1. Default date formats for all Target DB's are assumed to be the 'out-of-the-box' defaults.
2. Text fields to not contain the character "|", or the string "|,".
   * On some Target DBs, if you have text fields containing "|," (mssql) or "|" (sqlite), then the 'fast' import may fail, or insert bizarre values into your DB. This is due to the 'delimiter' which separates column values in the file that is sent to the Target DB.

# On Testing 
1. The 'Performance' matrix has been put together using a simple script which tests every combination of Source (5) and Target (5) DB migration (25 total combinations).
  * The script is not included (publicly), as it contains the connection strings of AWS RDS instances.
2. A regression test suite is needed, as is funding to setup an environment for Oracle and MSSQL instances.. 
3. There are definitely some untested column types here amongst all 5 RDBMS's. Please create *pull requests* or open *issues* that describe the problem **in detail** as these arise!


# Contributors
We are always looking for contributors! 

This project has [its origins](http://thelaziestprogrammer.com/migrating-between-databases-with-etlalchemy) in solving the problem of migrating off of bulky, expensive enterprise-level databases. If the project has helped you to migrate off of these databases, and onto open-source RDBMS's, the best way to show your support is by opening Pull Requests and Issues.



# Donations
[Donations through Gratipay](https://gratipay.com/etlalchemy/) are welcome, but **Pull Requests** are better!

You can also support us [via PayPal here.](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=DH544PY7RFSLA)

# Other

For help installing cx_Oracle on a Mac (El Capitan + cx_Oracle = Misery), [check out this blog post](http://thelaziestprogrammer.com/sharrington/databases/oracle/install-cx_oracle-mac) for help. 

Run this tool from the **same server that hosts your Target database** to get **maximum performance** out of it.
