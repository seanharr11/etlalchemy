.. :changelog:

History
-------

1.7 (2016-08-04)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New Features:

* Auto-determination of VARCHAR(n) column size. Size of VARCHAR(n) fields 
are auto-determined based on the # of chars in the largest string in the 
column, which is rounded up to the nearest power of 2. (i.e. 21 becomes 32).

Bug Fixes:

* Added more thorough UTF-8 support. Previous releases broke when unicode
strings were decoded as if they were byte-strings.
* Fixed bug which threw an exception when source is PostgreSQL, and table
is capitalized.

```engine.execute(SELECT COUNT(*) FROM Capitalizedtable)``` _replaced with_
```T_with_capitalized_name..count().fetchone() for cross-db support```

Other Changes:

* Created HISTORY.rst
