from .snflk import SNFLKQuery
from datetime import date

class StorageProfile(SNFLKQuery):

    ##TODO: Not able to parse JSON data accurately. Need to fix this query.
    def table_accessed(self,start_date="2022-01-01", end_date=""):

        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT query_id
        , query_start_time
        FROM {self.dbname}.ACCOUNT_USAGE.access_history
        , lateral flatten(base_objects_accessed) f1
        WHERE f1.value:"objectId"::int=32998411400350
        AND f1.value:"objectDomain"::string='Table'
        AND query_start_time >= dateadd('month', -6, current_timestamp())
        """
        df=self.query_to_df(sql)
        return df
