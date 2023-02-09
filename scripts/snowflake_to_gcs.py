import pickle
import json
import snowflake.connector
import logging
import datetime
from pandas import read_sql, DataFrame

# import pendulum

# ToDo: 1. Get data between two timestamp and save it to pickle or local csv format.

# from src.etl.lib.s3_utils import AnbS3BucketUtils, get_s3_bucket_credentials
# from src.etl.lib.log_utils import get_logger

# logger = get_logger(__name__)

user = "SARWAR"
password = "Reshu@123"
warehouse = "XSMALL_WH"
accountname = "VUJSGPR-XG91870"
role = "ACCOUNTADMIN"
database = 'SNOWFLAKE'


def read_params(param_path):
    with open(param_path) as fp:
        param_dict = json.load(fp)
    param_dict = {k.lower(): v for k, v in param_dict.items()}
    return param_dict


def get_query(params, **inputs):
    table = inputs['table'].lower()
    print(f"Getting query for table :: {table}")
    if table not in params:
        raise Exception("Input table is not available")

    start_time = datetime.datetime.strptime(inputs['end_time'], "%Y-%m-%d") - datetime.timedelta(
        days=inputs['timedelta'])

    # Case 1: When we have start and end time.
    if "START_TIME" in params[table] and "END_TIME" in params[table]:
        sql = f"""select * from {inputs["database"]}.{inputs['schema']}.{inputs['table']} where start_time >= '{start_time}' and end_time <= '{inputs['end_time']}'
                """
    # Case  2: Without start & end time
    # ToDo
    print("Input Query :: ", sql)
    return sql


def get_sf_conn() -> snowflake.connector:
    '''Returns a snowflake connection object.'''
    logging.info('returning snowflake connector')
    return snowflake.connector.connect(user=(user),
                                       password=(password),
                                       account=(accountname),
                                       warehouse=(warehouse),
                                       database=(database),
                                       role=(role))


def get_df(sql: str) -> DataFrame:
    '''Takes a SQL statement and returns a DataFrame.'''
    logging.info('getting dataframe from snowflake')
    with get_sf_conn() as conn:
        return read_sql(sql, conn)


def get_views(schema):
    sql = f"""
        show views in {schema};
        """
    df = get_df(sql)
    print(df)


def pickle_it(data, filename):
    pickle.dump(data, open(filename, "wb"))
    print(f"Pickled data saved to :: {filename}")


def unpickle_it(filepath):
    return pickle.load(open(filepath, "rb"))


def get_table_data(sql):
    df = get_df(sql)
    print(f"Data fetch complete with {df.shape[0]} rows.")
    df.to_csv("Query_table.csv", index=False)
    # print(df)
    return df


inputs = dict(
    param_path='params.json',
    database="SNOWFLAKE",
    schema='ACCOUNT_USAGE',
    table='QUERY_HISTORY',
    timedelta=7,
    end_time='2022-11-08',  # format: YYYY-MM-DD
    path_to_pickle="query_history_2022-11-08",
    do_pickle=True)

if __name__ == '__main__':
    # Read Params
    params = read_params(param_path=inputs['param_path'])
    # Create Query
    query = get_query(params, **inputs)
    # Create Snowflake Connection
    conn = get_sf_conn()
    # Get data from table
    df = get_table_data(query)
    if inputs['do_pickle']:
        pickle_it(data=df, filename=inputs['path_to_pickle'])
