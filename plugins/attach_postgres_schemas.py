import os

from dbt.adapters.duckdb.plugins import BasePlugin
from duckdb import DuckDBPyConnection

class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        libpq_conn_str = os.getenv("LIB_PQ_CONN_STR")
        schema1 = os.getenv("PG_SCHEMA")
        conn.execute(f"CALL postgres_attach('{libpq_conn_str}', source_schema='{schema1}', sink_schema='{schema1}')") 
