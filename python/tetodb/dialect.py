"""
TetoDB SQLAlchemy Dialect
Registers as 'tetodb' so you can use: create_engine("tetodb://host:port/database")
"""

from sqlalchemy.engine import default
from sqlalchemy import types as sa_types
from sqlalchemy import pool


class TetoDialect(default.DefaultDialect):
    name = "tetodb"
    driver = "tetodb"

    # Feature flags
    supports_alter = False
    supports_schemas = False
    supports_views = True
    supports_native_boolean = True
    supports_statement_cache = False
    supports_default_values = False
    supports_default_metavalue = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    postfetch_lastrowid = False
    implicit_returning = False
    preexecute_autoincrement_sequences = False

    # TetoDB doesn't use schema prefixes
    default_schema_name = None

    @classmethod
    def dbapi(cls):
        import tetodb.dbapi as module
        return module

    @classmethod
    def import_dbapi(cls):
        import tetodb.dbapi as module
        return module

    def create_connect_args(self, url):
        """
        Parse tetodb://host:port/database into connect() kwargs.
        """
        host = url.host or "127.0.0.1"
        port = url.port or 5432
        database = url.database or "e2e"
        return [], {"host": host, "port": port, "database": database}

    def do_ping(self, dbapi_connection):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
            return True
        except Exception:
            return False

    def has_table(self, connection, table_name, schema=None, **kwargs):
        """Check if a table exists by attempting a LIMIT 0 query."""
        try:
            connection.execute(
                f"SELECT * FROM {table_name} LIMIT 0;"
            )
            return True
        except Exception:
            return False

    def get_table_names(self, connection, schema=None, **kwargs):
        """TetoDB doesn't have information_schema; return empty list."""
        return []

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        """
        Retrieve column info by executing SELECT * ... LIMIT 0
        and reading cursor.description.
        """
        try:
            result = connection.execute(
                f"SELECT * FROM {table_name} LIMIT 0;"
            )
            columns = []
            if result.cursor.description:
                for col_desc in result.cursor.description:
                    name = col_desc[0]
                    type_name = col_desc[1] if col_desc[1] else "VARCHAR"
                    sa_type = self._map_type(type_name)
                    columns.append({
                        "name": name,
                        "type": sa_type,
                        "nullable": True,
                        "default": None,
                    })
            return columns
        except Exception:
            return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def _map_type(self, type_name):
        """Map TetoDB type name strings to SQLAlchemy types."""
        type_map = {
            "INTEGER": sa_types.Integer(),
            "BIGINT": sa_types.BigInteger(),
            "SMALLINT": sa_types.SmallInteger(),
            "TINYINT": sa_types.SmallInteger(),
            "BOOLEAN": sa_types.Boolean(),
            "DECIMAL": sa_types.Numeric(),
            "VARCHAR": sa_types.String(),
            "TEXT": sa_types.Text(),
            "CHAR": sa_types.CHAR(),
            "TIMESTAMP": sa_types.DateTime(),
        }
        return type_map.get(type_name.upper(), sa_types.String())

    def get_isolation_level(self, dbapi_connection):
        return "AUTOCOMMIT"

    def _get_server_version_info(self, connection):
        return (1, 0, 0)

    def _get_default_schema_name(self, connection):
        return None

    def do_begin(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("BEGIN;")
        cursor.close()

    def do_commit(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("COMMIT;")
        cursor.close()

    def do_rollback(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("ROLLBACK;")
        cursor.close()

    def get_schema_names(self, connection, **kwargs):
        return []


# Register the dialect
from sqlalchemy.dialects import registry
registry.register("tetodb", "tetodb.dialect", "TetoDialect")

dialect = TetoDialect
