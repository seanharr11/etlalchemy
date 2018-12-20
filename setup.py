import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass into pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ""

    def run_tests(self):
        import pytest
        import shlex

        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)

setup(
        name = 'etlalchemy',
        packages = ['etlalchemy'],
        version = '1.0.6',
        description = 'Extract, Transform, Load. Migrate any SQL Database in 4 lines of code',
        author = 'Sean Harrington',
        author_email='seanharr11@gmail.com',
        url='https://github.com/seanharr11/etlalchemy',
        download_url='https://github.com/seanharr11/etlalchemy/tarball/1.0.6',
        keywords=['sql','migration','etl','database'],
        install_requires = [
            "six>=1.9.0",
            "SQLAlchemy>=1.2.1,<1.3",
            "sqlalchemy-migrate>=0.9.7",
            "SQLAlchemy-Utils>=0.32.0"
        ],
        classifiers=[],
        cmdclass={'test': PyTest},
        tests_require = ["pytest"],
)
