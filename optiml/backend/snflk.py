import functools
import os
import pandas as pd
import pathlib

class SNFLKQuery():
    credit_values = {
    "standard": 2.0,
    "enterprise": 3.0,
    "business critical": 4.0
    }
    ##TODO: Set this as a class level property and capitalize
    data_type_map = ['float','float','string','datetime','datetime','string','datetime',
    'datetime','datetime','string','list','bytes','datetime','bool']

    def __init__(self, connection, dbname, credit_value="standard"):
    # def __init__(self, connection, dbname, cache, credit_value="standard"):
        self.connection = connection
        self.dbname = dbname
        self.credit_value = credit_value
        # self.cache = cache

    # def simple_cache(func):
    #     """Wraps each of the Snowflake query and returns results from cache if files exists locally.
    #     Else it runs the query and saves the results to a local file.

    #     Args:
    #         func (_type_): query function

    #     Returns:
    #         Dataframe: Pandas dataframe that contains the results of the query
    #     """
    #     @functools.wraps(func)
    #     def wrapper(self, *args, **kwargs):
    #         cache_file = f'{self.cache}/{func.__name__}.pq'
    #         if os.path.exists(cache_file):
    #             print('Loading data from cache...')
    #             df = pd.read_parquet(pathlib.Path(cache_file))
    #         else:
    #             print('Loading data from Snowflake...')
    #             df =  func(self, *args, **kwargs)

    #         df.to_parquet(cache_file)
    #         return df

    #     return wrapper

    ##TODO: Write this as a class level function instead of an instance level function
    def query_to_df(self, sql):
        cursor_obj = self.connection.cursor()
        data_one = cursor_obj.execute(sql).fetch_pandas_all()
        dt_type = {}
        for dd in cursor_obj.description:
            if SNFLKQuery.data_type_map[dd[1]] == "datetime":
                data_one[dd[0]] = pd.to_datetime(data_one[dd[0]])
            else:
                dt_type[dd[0]] = SNFLKQuery.data_type_map[dd[1]]
                data_one = data_one.astype({dd[0]: SNFLKQuery.data_type_map[dd[1]]})

        data_one.columns = data_one.columns.str.lower()
        return data_one