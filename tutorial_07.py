"""
As we add more data, query performance becomes an issue. Many small files could start to pile up
as our inserts vary in size. Each delete file becomes an extra bit of overhead that is paid every time
"""

import sqlalchemy as sa

from aws_iceberg_demo.catalog import get_catalog
# %%
from aws_iceberg_demo.connections import get_trino_engine

t = get_catalog().load_table("store.events")

# %%
"""
A common analytics query might be 'What is the most popular category per month'
"""

sql = "OPTIMIZE store.events REWRITE DATA USING BIN_PACK"

engine = get_trino_engine()

with engine.connect() as conn:
    r = conn.execute(sa.text(sql))
    print(r.fetchall())