from distutils.core import setup
setup(
        name = 'etlalchemy',
        packages = ['etlalchemy'],
        version = '1.0.7',
        description = 'Extract, Transform, Load. Migrate any SQL Database in 4 lines of code',
        author = 'Sean Harrington',
        author_email='seanharr11@gmail.com',
        url='https://github.com/seanharr11/etlalchemy',
        download_url='https://github.com/seanharr11/etlalchemy/tarball/1.0.7',
        keywords=['sql','migration','etl','database'],
        install_requires = [
            "six",
            "SQLAlchemy",
            "sqlalchemy-migrate",
            "SQLAlchemy-Utils"
        ],
        classifiers=[],
)
