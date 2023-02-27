from .snflk import SNFLKQuery
from datetime import date
import hashlib
import pandas as pd

class QueryProfile(SNFLKQuery):

    def append_query_hash(self, df):
        queries = df["query_text"].values.tolist()
        query_hashes = [hashlib.md5(q.encode()).hexdigest() for q in queries]
        df["query_hash"] = query_hashes
        return df

    def _aggregation(self, x):
        y = list(set(x))
        if len(y) == 1:
            y=y[0]

        return y

    def get_unique_queries(self, df):
        df = self.append_query_hash(df)
        df = self.append_query_hash(df)

        df_unique = df.groupby(["query_hash","query_text"]).agg({
                'user_name':lambda x:list(set(x)),
                'warehouse_name':lambda x:list(set(x)),
                'query_id':lambda x:list(x),
                'start_time': lambda x: list(x),
                'end_time': lambda x: list(x),
                'execution_status': 'count',
            })
        return df_unique

    def get_unique_queries_with_metrics_ordered(self, df, metric):
        df = self.append_query_hash(df)
        df_unique = df.groupby(["query_hash", "query_text"]).agg({
                'credits':'sum',
                'total_time_elapsed_sec': 'sum',
                'n_success': 'sum',
                'n_fail': 'sum',
                'user_name':lambda x:list(set(x)),
                'warehouse_name':lambda x:list(set(x)),
                'query_id':lambda x:list(x),
                'bytes_scanned': lambda x: list(x),
                'percentage_scanned_from_cache': lambda x: list(x),
                'bytes_spilled_to_local_storage': lambda x: list(x),
                'bytes_spilled_to_remote_storage': lambda x: list(x),
                'percentage_partitions_scanned': lambda x: list(x),
                'partitions_total': lambda x: list(x),
                'start_time': lambda x: list(x),
                'end_time': lambda x: list(x),
                'compilation_time_sec': lambda x: list(x),
                'execution_time_sec': lambda x: list(x),
                'queued_provisioning_time_sec': lambda x: list(x),
                'queued_repair_time_sec': lambda x: list(x),
                'queued_overload_time_sec': lambda x: list(x),
                'list_external_files_time_sec': lambda x: list(x),
                'execution_status': lambda x: list(x),

            })
        df_unique.sort_values(metric,inplace=True,ascending=False)
        df_unique.reset_index()
        return df_unique

    def n_inefficient_queries(self, start_date='', end_date='', n=10, metric='credits', unique=False):
        """Inefficient queries as order by metric. Metric options:
            bytes_scanned,
            percentage_scanned_from_cache,
            bytes_spilled_to_local_storage,
            bytes_spilled_to_remote_storage,
            percentage_partitions_scanned,
            partitions_total,
            total_time_elapsed_sec,
            compilation_time_sec,
            execution_time_sec,
            queued_provisioning_time_sec,
            queued_repair_time_sec,
            queued_overload_time_sec,
            list_external_files_time_sec,
            total_time_elapsed_sec,
            credits
        """
        
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]

        sql = f"""
        WITH WAREHOUSE_SIZE AS
        (
            SELECT WAREHOUSE_SIZE, NODES
            FROM (
                    SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
                    UNION ALL
                    SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
                    UNION ALL
                    SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
                    UNION ALL
                    SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
                    UNION ALL
                    SELECT 'XLARGE' AS WAREHOUSE_SIZE, 16 AS NODES
                    UNION ALL
                    SELECT '2XLARGE' AS WAREHOUSE_SIZE, 32 AS NODES
                    UNION ALL
                    SELECT '3XLARGE' AS WAREHOUSE_SIZE, 64 AS NODES
                    UNION ALL
                    SELECT '4XLARGE' AS WAREHOUSE_SIZE, 128 AS NODES
                    )
        )
            select qh.query_id,
            qh.query_type,
            qh.query_text,
            qh.user_name,
            qh.role_name,
            qh.database_name,
            qh.schema_name,
            qh.warehouse_name,
            qh.warehouse_size,
            qh.warehouse_type,
            qh.bytes_scanned,
            round(qh.percentage_scanned_from_cache*100,2) as percentage_scanned_from_cache,
            qh.bytes_spilled_to_local_storage,
            qh.bytes_spilled_to_remote_storage,
            round(qh.partitions_scanned/qh.partitions_total*100,2) as percentage_partitions_scanned,
            qh.partitions_total,
            qh.start_time,
            qh.end_time,
            round((qh.total_elapsed_time/(1000)),2) as total_time_elapsed_sec,
            round((qh.compilation_time/(1000)),2) as compilation_time_sec,
            round((qh.execution_time/1000),2) as execution_time_sec,
            round((qh.queued_provisioning_time/1000),2) as queued_provisioning_time_sec,
            round((qh.queued_repair_time/1000),2) as queued_repair_time_sec,
            round((qh.queued_overload_time/1000),2) as queued_overload_time_sec,
            round((qh.list_external_files_time/1000),2) as list_external_files_time_sec,
            qh.query_tag,
            qh.execution_status,
            round((qh.execution_time/(1000*60*60))*ws.nodes,2) as credits

        from {self.dbname}.QUERY_HISTORY QH
        join WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        where date_trunc('day', qh.start_time) between '{start_date}' and '{end_date}'
        order by {metric} desc
        limit {n}   
        """
        df = self.query_to_df(sql)
        df["n_success"] = (df["execution_status"] == "SUCCESS").astype(int)
        df["n_fail"] = (df["execution_status"] == "FAIL").astype(int)
        return df


    def queries_by_execution_status(self, start_date='', end_date=''):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]

        
        sql = f"""
            select
                qh.user_name, 
                qh.execution_status, 
                qh.warehouse_name,
                date_trunc('day', qh.start_time) as day,
                count(*) as counts
            from 
                {self.dbname}.query_history qh
            where 
                date_trunc('day', qh.start_time) between '{start_date}' and '{end_date}'
            group by 
                qh.user_name, 
                qh.execution_status, 
                qh.warehouse_name,
                day
            order by 
                user_name, day;
        """

        df = self.query_to_df(sql)
        df = df.fillna("Unassigned")
        df["n_success"] = (df["execution_status"] == "SUCCESS").astype(int)*df["counts"]
        df["n_fail"] = (df["execution_status"] == "FAIL").astype(int)*df["counts"]
        return df


    
    def n_most_executed_select_queries(self, start_date='2022-01-01',end_date='', n=10):
        """
       Shows the most executed SELECT queries.
       Outputs a dataframe with the following columns:
        QUERY_TEXT: Text of SQL statement.
        QUERY_TYPE:DML, query, etc. If the query failed, then the query type may be UNKNOWN.
        NUMBER_OF_QUERIES: Number of times the SELECT query is executed.
        USER_NAME: User who issued the query.
        EXECUTION_MINUTES: execution time of query in minutes.
        WAREHOUSE_NAME: Warehouse that the query executed on.
        BYTES_SPILLED_TO_LOCAL_STORAGE: Volume of data spilled to local disk.
        BYTES_SPILLED_TO_REMOTE_STORAGE: Volume of data spilled to remote disk.
        PARTITIONS_SCANNED: Number of micro-partitions scanned.
        PARTITIONS_TOTAL: Total micro-partitions of all tables included in this query.
        CLUSTER_NUMBER: The cluster (in a multi-cluster warehouse) that this statement executed on.
        COMPILATION_TIME_SEC: Compilation time (in seconds).
        CREDITS: Credits consumed by the query.
        EXECUTION_STATUS: Execution status for the query. Valid values: success, fail, incident.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""

        SELECT 
        Q.QUERY_TEXT
        ,Q.QUERY_TYPE
        ,count(*) as number_of_queries
        ,Q.user_name
        ,Q.warehouse_name
        ,sum(Q.BYTES_SPILLED_TO_LOCAL_STORAGE) as BYTES_SPILLED_TO_LOCAL_STORAGE
        ,sum(Q.BYTES_SPILLED_TO_REMOTE_STORAGE) as BYTES_SPILLED_TO_REMOTE_STORAGE
        ,sum(Q.TOTAL_ELAPSED_TIME)/1000 as execution_seconds
        ,sum(Q.TOTAL_ELAPSED_TIME)/(1000*60) as execution_minutes
        ,sum(Q.TOTAL_ELAPSED_TIME)/(1000*60*60) as execution_hours
        ,sum(Q.PARTITIONS_SCANNED) as PARTITIONS_SCANNED
        ,sum(Q.PARTITIONS_TOTAL) as PARTITIONS_TOTAL
        ,max(Q.cluster_number)
        from {self.dbname}.QUERY_HISTORY Q
        where 1=1
        and QUERY_TYPE='SELECT'
        and TO_DATE(Q.START_TIME) between '{start_date}' and '{end_date}'
        and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
        group by 1,2,4,5
        having count(*) >= 10 --configurable/minimal threshold
        order by 3 desc
        limit {n} --configurable upper bound threshold
        ;
        """
        df=self.query_to_df(sql)
        df["average_execution_time_milliseconds"]=(df["execution_seconds"]*1000)/df["number_of_queries"]
        return df


    def queries_full_table_scan(self, start_date='2022-01-01',end_date='',n=10):
        """
        Shows the queries with near full table scans
        Outputs a dataframe with all the columns in QUERY_HISTORY
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT * 
        FROM {self.dbname}.QUERY_HISTORY
        WHERE START_TIME between '{start_date}' and '{end_date}'
        AND PARTITIONS_SCANNED > (PARTITIONS_TOTAL*0.95)
        AND QUERY_TYPE NOT LIKE 'CREATE%'
        ORDER BY PARTITIONS_SCANNED DESC
        LIMIT {n}  -- Configurable threshold that defines "TOP N=50"
        ;
        """
        df=self.query_to_df(sql)
        return df

    def queries_by_wh(self,start_datetime="2022-01-01", end_datetime="", warehouse_name='DEV_WH'):
        """
        Shows queries issued in specific warehouse in given time period.
        """
        # if not end_date:
        #     today_date = date.today()
        #     end_date = str(today_date)
        sql=f"""
        WITH WAREHOUSE_SIZE AS
        (
            SELECT WAREHOUSE_SIZE, NODES
            FROM (
                    SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
                    UNION ALL
                    SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
                    UNION ALL
                    SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
                    UNION ALL
                    SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
                    UNION ALL
                    SELECT 'XLARGE' AS WAREHOUSE_SIZE, 16 AS NODES
                    UNION ALL
                    SELECT '2XLARGE' AS WAREHOUSE_SIZE, 32 AS NODES
                    UNION ALL
                    SELECT '3XLARGE' AS WAREHOUSE_SIZE, 64 AS NODES
                    UNION ALL
                    SELECT '4XLARGE' AS WAREHOUSE_SIZE, 128 AS NODES
                    )
        )
        select qh.query_id,
        qh.query_type,
        qh.query_text,
        qh.user_name,
        qh.role_name,
        qh.database_name,
        qh.schema_name,
        qh.warehouse_name,
        qh.warehouse_size,
        qh.warehouse_type,
        qh.bytes_scanned,
        round(qh.percentage_scanned_from_cache*100,2) as percentage_scanned_from_cache,
        qh.bytes_spilled_to_local_storage,
        qh.bytes_spilled_to_remote_storage,
        qh.partitions_total,
        qh.start_time,
        qh.end_time,
        qh.execution_status,
        round((qh.total_elapsed_time/(1000)),2) as total_time_elapsed_sec,
        round((qh.compilation_time/(1000)),2) as compilation_time_sec,
        round((qh.execution_time/1000),2) as execution_time_sec,
        round((qh.queued_provisioning_time/1000),2) as queued_provisioning_time_sec,
        round((qh.queued_repair_time/1000),2) as queued_repair_time_sec,
        round((qh.queued_overload_time/1000),2) as queued_overload_time_sec,
        round((qh.list_external_files_time/1000),2) as list_external_files_time_sec,
        ROUND((EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS
        FROM {self.dbname}.QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        where START_TIME between '{start_datetime}' and '{end_datetime}'
        and warehouse_name='{warehouse_name}'
        order by credits desc
        """
        df=self.query_to_df(sql)
        return df

    def query_id_to_details(self, query_id):
        sql = f"""
            select * 
            from {self.dbname}.query_history
            WHERE query_id='{query_id}'
        """
        df = self.query_to_df(sql)
        return df

    # def get_queries_by_wh(self,\
    #                 start_datetime=None,\
    #                 end_datetime=None,\
    #                 warehouse_name=None):
    #     """Gets queries running at a particular time, by a specific user, 
    #     in a specific warehouse """
        
    #     # start_datetime = start_datetime or "start_datetime"
    #     # end_datetime = end_datetime or "end_datetime"
    #     # user_name = user_name or "user_name"
    #     # warehouse_name = warehouse_name or 'warehouse_name'

    #     sql = f"""
    #         select qh.query_id,
    #         qh.query_type,
    #         qh.query_text,
    #         qh.user_name,
    #         qh.role_name,
    #         qh.database_name,
    #         qh.schema_name,
    #         qh.warehouse_name,
    #         qh.warehouse_size,
    #         qh.warehouse_type,
    #         qh.bytes_scanned,
    #         round(qh.percentage_scanned_from_cache*100,2) as percentage_scanned_from_cache,
    #         qh.bytes_spilled_to_local_storage,
    #         qh.bytes_spilled_to_remote_storage,
    #         qh.partitions_total,
    #         qh.start_time,
    #         qh.end_time,
    #         qh.query_tag,
    #         qh.execution_status,
    #         round((qh.total_elapsed_time/(1000)),2) as total_time_elapsed_sec,
    #         round((qh.compilation_time/(1000)),2) as compilation_time_sec,
    #         round((qh.execution_time/1000),2) as execution_time_sec,
    #         round((qh.queued_provisioning_time/1000),2) as queued_provisioning_time_sec,
    #         round((qh.queued_repair_time/1000),2) as queued_repair_time_sec,
    #         round((qh.queued_overload_time/1000),2) as queued_overload_time_sec,
    #         round((qh.list_external_files_time/1000),2) as list_external_files_time_sec
    #             from {self.dbname}.query_history qh
    #             where qh.start_time between '{start_datetime}' and '{end_datetime}'
    #             and qh.warehouse_name='{warehouse_name}'
    #         """
        
    #     df = self.query_to_df(sql)
    #     return df


    # def get_queries_by_wh_with_credits(self,\
    #                 start_datetime=None,\
    #                 end_datetime=None,\
    #                 warehouse_name=None):
    #     """Gets queries running at a particular time, by a specific user, 
    #     in a specific warehouse """
        
    #     # start_datetime = start_datetime or "start_datetime"
    #     # end_datetime = end_datetime or "end_datetime"
    #     # user_name = user_name or "user_name"
    #     # warehouse_name = warehouse_name or 'warehouse_name'

    #     sql = f"""
    #         select qh.query_id,
    #         qh.query_type,
    #         qh.query_text,
    #         qh.user_name,
    #         qh.role_name,
    #         qh.database_name,
    #         qh.schema_name,
    #         qh.warehouse_name,
    #         qh.warehouse_size,
    #         qh.warehouse_type,
    #         qh.bytes_scanned,
    #         round(qh.percentage_scanned_from_cache*100,2) as percentage_scanned_from_cache,
    #         qh.bytes_spilled_to_local_storage,
    #         qh.bytes_spilled_to_remote_storage,
    #         qh.partitions_total,
    #         qh.start_time,
    #         qh.end_time,
    #         qh.query_tag,
    #         qh.execution_status,
    #         round((qh.total_elapsed_time/(1000)),2) as total_time_elapsed_sec,
    #         round((qh.compilation_time/(1000)),2) as compilation_time_sec,
    #         round((qh.execution_time/1000),2) as execution_time_sec,
    #         round((qh.queued_provisioning_time/1000),2) as queued_provisioning_time_sec,
    #         round((qh.queued_repair_time/1000),2) as queued_repair_time_sec,
    #         round((qh.queued_overload_time/1000),2) as queued_overload_time_sec,
    #         round((qh.list_external_files_time/1000),2) as list_external_files_time_sec
    #             from {self.dbname}.query_history qh
    #             where qh.start_time between '{start_datetime}' and '{end_datetime}'
    #             and qh.warehouse_name='{warehouse_name}'
    #         """
        
    #     df = self.query_to_df(sql)
    #     return df
    


    def n_inefficient_queries_v2(self, start_date='', end_date='', n=10, metric='credits', unique=False):        
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]

        sql = f"""
        WITH WAREHOUSE_SIZE AS
        (
            SELECT WAREHOUSE_SIZE, NODES
            FROM (
                    SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
                    UNION ALL
                    SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
                    UNION ALL
                    SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
                    UNION ALL
                    SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
                    UNION ALL
                    SELECT 'XLARGE' AS WAREHOUSE_SIZE, 16 AS NODES
                    UNION ALL
                    SELECT '2XLARGE' AS WAREHOUSE_SIZE, 32 AS NODES
                    UNION ALL
                    SELECT '3XLARGE' AS WAREHOUSE_SIZE, 64 AS NODES
                    UNION ALL
                    SELECT '4XLARGE' AS WAREHOUSE_SIZE, 128 AS NODES
                    )
        )
            select 
            HASH(qh.query_text) as query_hash,
            qh.query_text,
            qh.warehouse_name,
            qh.user_name,
            sum(round((qh.execution_time/(1000*60*60))*ws.nodes,2)) as credits
        from {self.dbname}.QUERY_HISTORY qh
        join WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        where date_trunc('day', qh.start_time) between '{start_date}' and '{end_date}'
        group by 1,2,3,4
        order by {metric} desc
        limit {n}   
        """
        df = self.query_to_df(sql)
        return df


    def n_most_executed_select_queries_v2(self, start_date='',end_date='', n=10):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""

        SELECT 
        HASH(Q.QUERY_TEXT) as query_hash
        ,Q.QUERY_TEXT
        ,Q.warehouse_name
        ,Q.user_name
        ,count(*) as number_of_queries
        ,sum(Q.TOTAL_ELAPSED_TIME)/1000 as execution_seconds
        ,execution_seconds/number_of_queries as average_execution_seconds
        from {self.dbname}.QUERY_HISTORY Q
        where 
        QUERY_TYPE='SELECT'
        and TO_DATE(Q.START_TIME) between '{start_date}' and '{end_date}'
        and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
        group by 1,2,3,4
        having number_of_queries >= 10 --configurable/minimal threshold
        and execution_seconds/number_of_queries > 5 --only queries that run for over 5 seconds
        order by 4 desc
        limit {n} --configurable upper bound threshold
        ;
        """
        df=self.query_to_df(sql)
        df['query_hash'] = df['query_hash'].apply(str)
        return df
    