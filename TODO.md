1. Add regression tests
2. Add unit tests
3. Add support for Python 3.5
4. Check to see if FK exists between 2 tables - if it does, append an integer to the end of the constraint_name. Right now we just catch exceptions, and only on a subset of supported RDBMSs (OperationalError - MySQL, ProgrammingError - PostgreSQL)
5. Replace column-type guessing process (iterating over every row)  with a GROUP BY query to improve performance.
6. Add parameter for "_quoted_strings_enclosed_by_" to **ETLAlchemySource** to override default .csv quote-character.
7. Add parameter for "_cleanup_table_csv_files_", with default value of True, allowing the user to override default and let files persist after they are loaded into Target DB.
