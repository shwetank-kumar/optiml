from .snflk import SNFLKQuery
from datetime import date
import hashlib
import pandas as pd

class QueryProfile(SNFLKQuery):

    def _append_query_hash(self, df):
        queries = df["query_text"].values.tolist()
        query_hashes = [hashlib.md5(q.encode()).hexdigest() for q in queries]
        df["query_hash"] = query_hashes
        return df

    def _aggregation(self, x):
        y = list(set(x))
        if len(y) == 1:
            y=y[0]

        return y

    def get_unique_queries(self, df, metric):
        df = self._append_query_hash(df)
        df1 = pd.DataFrame()
        df1 = df.groupby(['query_hash']).agg({
                metric:'mean',
                'credits':'sum',
                'n_success': 'sum',
                'n_fail': 'sum',
                'query_id':lambda x:list(x)}).reset_index()
        df1.rename(columns={metric: "avg_" + metric, "credits": "total_credits"}, inplace=True)
        df1.sort_values("total_credits", inplace=True, ascending=False)
        df1.reset_index(inplace=True, drop=True)
        return df1

    def n_inefficient_queries(self, start_date='', end_date='', n=10, metric='credits', unique=False):
        """Inefficient queries as order by metric. Metric options:
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

        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
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
                {self.dbname}.account_usage.query_history qh
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
        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY Q
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
        Outputs a dataframe with all the columns in ACCOUNT_USAGE.QUERY_HISTORY
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT * 
        FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME between '{start_date}' and '{end_date}'
        AND PARTITIONS_SCANNED > (PARTITIONS_TOTAL*0.95)
        AND QUERY_TYPE NOT LIKE 'CREATE%'
        ORDER BY PARTITIONS_SCANNED DESC
        LIMIT {n}  -- Configurable threshold that defines "TOP N=50"
        ;
        """
        df=self.query_to_df(sql)
        return df

    def queries_by_wh(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH'):
        """
        Shows queries issued in specific warehouse in given time period.

        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
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
        SELECT QUERY_ID,
        EXECUTION_TIME,
        START_TIME,
        ROUND((EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS,
        COMPILATION_TIME,
        TOTAL_ELAPSED_TIME,
        QUEUED_OVERLOAD_TIME,
        QUEUED_PROVISIONING_TIME,
        QUEUED_REPAIR_TIME
        FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        where START_TIME between '{start_date}' and '{end_date}'
        and warehouse_name='{wh_name}'
        order by execution_time desc
        """
        df=self.query_to_df(sql)
        return df

    def query_id_to_details(self, query_id):
        sql = f"""
            select * 
            from {self.dbname}.account_usage.query_history
            WHERE query_id='{query_id}'
        """
        df = self.query_to_df(sql)
        return df

