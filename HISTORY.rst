.. :changelog:

History
-------

1.1.1 (2017-05-15)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**New Features**:

* ``compress_varchar`` parameter added to ETLAlchemySource.__init__() to allow for optional "_minimizing of varchar() columns_". Defaults to ``False``.

**Bug Fixes**:

* Handles huge Decimal values ( > 2^32 ) when determining whether or not to coerce column type to Integer, or leave Decimal.

* Fixed bugs surrounding **Upserting** of rows (when ``drop_database=False``).

* Nearest power of 2 now rounds properly


1.0.7 (2016-08-04)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**New Features**:

* Auto-determination of VARCHAR(n) column size. Size of VARCHAR(n) fields are auto-determined based on the # of chars in the largest string in the column, which is rounded up to the nearest power of 2. (i.e. 21 becomes 32).

**Bug Fixes**:

* Added more thorough UTF-8 support. Previous releases broke when unicode strings were decoded as if they were byte-strings.

* Fixed bug which threw an exception when source is PostgreSQL, and table is capitalized.

``engine.execute(SELECT COUNT(*) FROM Capitalizedtable)`` *replaced with*
``T_with_capitalized_name..count().fetchone() for cross-db support``


**Other Changes**:

* Created HISTORY.rst
