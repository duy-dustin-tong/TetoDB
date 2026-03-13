from setuptools import setup, find_packages

setup(
    name="tetodb",
    version="1.0.0",
    description="TetoDB Python Driver and SQLAlchemy Dialect",
    packages=find_packages(),
    install_requires=["sqlalchemy>=2.0"],
    entry_points={
        "sqlalchemy.dialects": [
            "tetodb = tetodb.dialect:TetoDialect",
        ],
    },
)
