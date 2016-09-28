1. Check to see if FK exists between 2 tables - if it does, append an integer to the end of the constraint_name.
Right now we just catch exceptions (OperationalError - MySQL, ProgrammingError - PostgreSQL)
2. Replace column-type guessing process (iterating over every row)  with a GROUP BY query.
3. Add parameter for "quoted_strings_enclosed_by" to **ETLAlchemySource** to override default .csv quote-character.
4. Add parameter for "cleanup_table_csv_files", with default value of True, allowing the user to override default and let files persist.
