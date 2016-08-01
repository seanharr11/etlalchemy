sqlalchemy-migrate
==================

Fork from http://code.google.com/p/sqlalchemy-migrate/ to get it working with
SQLAlchemy 0.8.

Inspired by Ruby on Rails' migrations, Migrate provides a way to deal with
database schema changes in `SQLAlchemy <http://sqlalchemy.org>`_ projects.

Migrate extends SQLAlchemy to have database changeset handling. It provides a
database change repository mechanism which can be used from the command line as
well as from inside python code.

Help
----

Sphinx documentation is available at the project page `packages.python.org
<http://packages.python.org/sqlalchemy-migrate/>`_.

Users and developers can be found at #sqlalchemy-migrate on Freenode IRC
network and at the public users mailing list `migrate-users
<http://groups.google.com/group/migrate-users>`_.

New releases and major changes are announced at the public announce mailing
list `migrate-announce <http://groups.google.com/group/migrate-announce>`_
and at the Python package index `sqlalchemy-migrate
<http://pypi.python.org/pypi/sqlalchemy-migrate>`_.

Homepage is located at `stackforge
<http://github.com/stackforge/sqlalchemy-migrate/>`_

You can also clone a current `development version
<http://github.com/stackforge/sqlalchemy-migrate>`_

Tests and Bugs
--------------

To run automated tests:

* Copy test_db.cfg.tmpl to test_db.cfg
* Edit test_db.cfg with database connection strings suitable for running tests.
  (Use empty databases.)
* $ pip install -r requirements.txt -r test-requirements.txt
* $ python setup.py develop
* $ testr run --parallel

Please report any issues with sqlalchemy-migrate to the issue tracker at
`code.google.com issues
<http://code.google.com/p/sqlalchemy-migrate/issues/list>`_



