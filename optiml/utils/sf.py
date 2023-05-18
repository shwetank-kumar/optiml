import snowflake.connector
import os
import pandas as pd
import sys
from importlib import reload  # Not needed in Python 2
import logging
import functools
import time
from snowflake.snowpark.session import Session

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)

def snowconn():
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        role=os.environ['SNOWFLAKE_ROLE'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        client_session_keep_alive=True
    )
    print(f"connected to account {conn.account} wh {conn.warehouse} db {conn.database} schema {conn.schema} with role {conn.role}")
    return conn

session_creds = {
    "user": os.environ['SNOWFLAKE_USER'],
    "password": os.environ['SNOWFLAKE_PASSWORD'],
    "account": os.environ['SNOWFLAKE_ACCOUNT'],
    "role": os.environ['SNOWFLAKE_ROLE'],
    "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],
    "database": os.environ['SNOWFLAKE_DATABASE'],
    "schema": os.environ['SNOWFLAKE_SCHEMA'],
}

def snowsession():
    session = Session.builder.configs(session_creds).create()
    return session


conn = snowconn()
session = snowsession()

def run_sql(sql: str, ctx=conn, wait=True):
    logging.debug(f"running sql: {sql}")
    return conn.cursor().execute(sql, _no_results=(not wait))


# @functools.cache
def sql_to_df(sql_query, pre_hook=[], ctx=conn):
    if len(pre_hook):
        logging.debug(f"RUNNING pre-hook: {pre_hook}")
    for s in pre_hook:
        run_sql(s,conn)
        # print(f"RUNNING SQL: {sql_query}")

    # todo: move to latest method of pandas dataframe fetching
    # may need to upgrade python: https://github.com/snowflakedb/snowflake-connector-python/issues/986#issuecomment-1115354587
    
    trimmed_lowered = sql_query.strip().lower()
    if trimmed_lowered.startswith('select') or trimmed_lowered.startswith('with'):
        print(f"using arrow to fetch results...")
        cur = ctx.cursor()
        cur.execute(sql_query)
        data = cur.fetch_pandas_all() 
        cur.close()
    else:
        data = pd.read_sql(
            sql_query,
            ctx,
        )
    
    data.columns = data.columns.str.lower()
    return data