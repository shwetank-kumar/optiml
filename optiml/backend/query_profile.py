from .snflk import SNFLKQuery
from datetime import date

class QueryProfile(SNFLKQuery):
    ## Query cost related queries
    ##TODO: 1) Check query 2) Add flag to give unique query text with parameters
    def n_expensive_queries(self, start_date='2022-01-01', end_date='', n=10):
        """
        Calculates expense of queries over a specific time period.
        Outputs a dataframe with the following columns:
        QUERY_TEXT: Text of SQL statement.
        QUERY_ID: Internal/system-generated identifier for the SQL statement.
        USER_NAME: User who issued the query.
        ROLE_NAME: Role that was active in the session at the time of the query.
        START_TIME: Statement start time.
        END_TIME: Statement end time.
        EXECUTION_TIME_MINUTES: execution time of query in minutes.
        WAREHOUSE_NAME: Warehouse that the query executed on.
        BYTES_SCANNED: Number of bytes scanned by this statement.
        PERCENTAGE_SCANNED_FROM_CACHE: The percentage of data scanned from the local disk cache.
        BYTES_SPILLED_TO_LOCAL_STORAGE: Volume of data spilled to local disk.
        BYTES_SPILLED_TO_REMOTE_STORAGE: Volume of data spilled to remote disk.
        PARTITIONS_SCANNED: Number of micro-partitions scanned.
        PARTITIONS_TOTAL: Total micro-partitions of all tables included in this query.
        WAREHOUSE_SIZE: Size of the warehouse the queries are executed on.
        NODES : Node value associated with the warehouse the query is being executed on.
        COMPILATION_TIME: Compilation time (in seconds).
        CREDITS: Credits consumed by the query.
        EXECUTION_STATUS: Execution status for the query. Valid values: success, fail, incident.
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
                        SELECT 'XSMALL' AS WAREHOUSE_SIZE, 1 AS NODES
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
            ),
            QUERY_HISTORY AS
            (
                SELECT QH.QUERY_ID
                    ,QH.QUERY_TEXT
                    ,QH.USER_NAME
                    ,QH.ROLE_NAME
                    ,QH.COMPILATION_TIME
                    ,QH.START_TIME
                    ,QH.END_TIME
                    ,QH.WAREHOUSE_NAME
                    ,QH.EXECUTION_TIME
                    ,QH.WAREHOUSE_SIZE
                    ,QH.BYTES_SCANNED
                    ,QH.PERCENTAGE_SCANNED_FROM_CACHE
                    ,QH.BYTES_SPILLED_TO_LOCAL_STORAGE
                    ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
                    ,QH.PARTITIONS_SCANNED
                    ,QH.PARTITIONS_TOTAL
                    ,QH.EXECUTION_STATUS
                FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
                WHERE START_TIME between '{start_date}' and '{end_date}'
            )

            SELECT QH.QUERY_ID
                ,QH.QUERY_TEXT
                ,QH.USER_NAME
                ,QH.ROLE_NAME
                ,QH.START_TIME
                ,QH.END_TIME
                ,ROUND(QH.EXECUTION_TIME/(1000*60),2) AS EXECUTION_TIME_MINUTES
                ,QH.WAREHOUSE_NAME
                ,QH.BYTES_SCANNED
                ,QH.PERCENTAGE_SCANNED_FROM_CACHE
                ,QH.BYTES_SPILLED_TO_LOCAL_STORAGE
                ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
                ,QH.PARTITIONS_SCANNED
                ,QH.PARTITIONS_TOTAL
                ,WS.WAREHOUSE_SIZE
                ,WS.NODES
                ,ROUND((QH.COMPILATION_TIME/(1000)),2) AS COMPILATION_TIME_SEC
                ,ROUND((QH.EXECUTION_TIME/(1000*60)),2) AS EXECUTION_TIME_MIN
                ,ROUND((QH.EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS
                ,QH.EXECUTION_STATUS

            FROM QUERY_HISTORY QH
            JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
            ORDER BY CREDITS DESC
            LIMIT {n}
            ;
        """
        df = self.query_to_df(sql)
        return df

    ##TODO: Update query output columns to be same as expensive queries
    def n_queries_spill_to_storage(self, start_date='2022-01-01', end_date='', n=10):
        """
        Shows queries spilling maximum remote storage
        Outputs a dataframe with the following columns:
        QUERY_TEXT: Text of SQL statement.
        QUERY_TYPE:DML, query, etc. If the query failed, then the query type may be UNKNOWN.
        QUERY_ID: Internal/system-generated identifier for the SQL statement.
        USER_NAME: User who issued the query.
        ROLE_NAME: Role that was active in the session at the time of the query.
        DATABASE_NAME: Database that was in use at the time of the query
        SCHEMA_NAME: Schema that was in use at the time of the query
        EXECUTION_TIME_MINUTES: execution time of query in minutes.
        WAREHOUSE_NAME: Warehouse that the query executed on.
        BYTES_SPILLED_TO_LOCAL_STORAGE: Volume of data spilled to local disk.
        BYTES_SPILLED_TO_REMOTE_STORAGE: Volume of data spilled to remote disk.
        PARTITIONS_SCANNED: Number of micro-partitions scanned.
        PARTITIONS_TOTAL: Total micro-partitions of all tables included in this query.
        WAREHOUSE_SIZE: Size of the warehouse the queries are executed on.
        CLUSTER_NUMBER: The cluster (in a multi-cluster warehouse) that this statement executed on.
        COMPILATION_TIME_SEC: Compilation time (in seconds).
        CREDITS: Credits consumed by the query.
        EXECUTION_STATUS: Execution status for the query. Valid values: success, fail, incident.

        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
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
       

        SELECT QH.QUERY_ID
            ,QH.QUERY_TYPE
            ,QH.QUERY_TEXT
            ,QH.USER_NAME
            ,QH.ROLE_NAME
            ,QH.DATABASE_NAME
            ,QH.SCHEMA_NAME
            ,QH.WAREHOUSE_NAME
            ,QH.WAREHOUSE_SIZE
            ,QH.BYTES_SPILLED_TO_LOCAL_STORAGE
            ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
            ,QH.PARTITIONS_SCANNED
            ,QH.PARTITIONS_TOTAL
            ,ROUND((QH.COMPILATION_TIME/(1000)),2) AS COMPILATION_TIME_SEC
            ,ROUND((QH.EXECUTION_TIME/(1000*60)),2) AS EXECUTION_TIME_MIN
            ,ROUND((QH.EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS
            ,QH.CLUSTER_NUMBER
            ,QH.EXECUTION_STATUS
        from  {self.dbname}.account_usage.query_history QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
       
        
        where  BYTES_SPILLED_TO_REMOTE_STORAGE > 0
        and TO_DATE(start_time) between '{start_date}' and '{end_date}'
        
        order  by BYTES_SPILLED_TO_REMOTE_STORAGE desc
        limit {n};
        """
        df = self.query_to_df(sql)
        return df
    
    ##TODO: Update query output columns to be same as expensive queries
    def n_queries_scanned_most_data(self, start_date='2022-01-01',end_date='2022-02-02',n=10):
        """
        Shows queries that scan the most data
        Outputs a dataframe with the following columns:
        QUERY_TEXT: Text of SQL statement.
        QUERY_TYPE:DML, query, etc. If the query failed, then the query type may be UNKNOWN.
        QUERY_ID: Internal/system-generated identifier for the SQL statement.
        USER_NAME: User who issued the query.
        ROLE_NAME: Role that was active in the session at the time of the query.
        DATABASE_NAME: Database that was in use at the time of the query
        SCHEMA_NAME: Schema that was in use at the time of the query
        EXECUTION_TIME_MINUTES: execution time of query in minutes.
        WAREHOUSE_NAME: Warehouse that the query executed on.
        BYTES_SPILLED_TO_LOCAL_STORAGE: Volume of data spilled to local disk.
        BYTES_SPILLED_TO_REMOTE_STORAGE: Volume of data spilled to remote disk.
        PARTITIONS_SCANNED: Number of micro-partitions scanned.
        PARTITIONS_TOTAL: Total micro-partitions of all tables included in this query.
        WAREHOUSE_SIZE: Size of the warehouse the queries are executed on.
        CLUSTER_NUMBER: The cluster (in a multi-cluster warehouse) that this statement executed on.
        COMPILATION_TIME_SEC: Compilation time (in seconds).
        CREDITS: Credits consumed by the query.
        EXECUTION_STATUS: Execution status for the query. Valid values: success, fail, incident.

        """
        
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql= f"""
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

        SELECT QH.QUERY_ID
            ,QH.QUERY_TYPE
            ,QH.QUERY_TEXT
            ,QH.USER_NAME
            ,QH.ROLE_NAME
            ,QH.DATABASE_NAME
            ,QH.SCHEMA_NAME
            ,QH.WAREHOUSE_NAME
            ,QH.WAREHOUSE_SIZE
            ,QH.BYTES_SPILLED_TO_LOCAL_STORAGE
            ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
            ,QH.PARTITIONS_SCANNED
            ,QH.PARTITIONS_TOTAL
            ,ROUND((QH.COMPILATION_TIME/(1000)),2) AS COMPILATION_TIME_SEC
            ,ROUND((QH.EXECUTION_TIME/(1000*60)),2) AS EXECUTION_TIME_MIN
            ,ROUND((QH.EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS
            ,QH.CLUSTER_NUMBER
            ,QH.EXECUTION_STATUS
        

        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        where 1=1
        and TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
            and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
            and ERROR_CODE iS NULL
            and PARTITIONS_SCANNED is not null
        
        order by PARTITIONS_SCANNED desc
        LIMIT {n}
        """
        df=self.query_to_df(sql)
        return df
    
    ##TODO: Update query output columns to be same as expensive queries
    def n_most_cached_queries(self, start_date='2022-01-01',end_date='', n=10):
        """
        Shows the top queries that scanned high percentage of data from cache
        Outputs a dataframe with the following columns:
        QUERY_TEXT: Text of SQL statement.
        QUERY_TYPE:DML, query, etc. If the query failed, then the query type may be UNKNOWN.
        QUERY_ID: Internal/system-generated identifier for the SQL statement.
        USER_NAME: User who issued the query.
        ROLE_NAME: Role that was active in the session at the time of the query.
        DATABASE_NAME: Database that was in use at the time of the query
        SCHEMA_NAME: Schema that was in use at the time of the query
        EXECUTION_TIME_MINUTES: execution time of query in minutes.
        WAREHOUSE_NAME: Warehouse that the query executed on.
        BYTES_SPILLED_TO_LOCAL_STORAGE: Volume of data spilled to local disk.
        BYTES_SPILLED_TO_REMOTE_STORAGE: Volume of data spilled to remote disk.
        PARTITIONS_SCANNED: Number of micro-partitions scanned.
        PARTITIONS_TOTAL: Total micro-partitions of all tables included in this query.
        PERCENTAGE_SCANNED_FROM_CACHE: The percentage of data scanned from the local disk cache.
        WAREHOUSE_SIZE: Size of the warehouse the queries are executed on.
        CLUSTER_NUMBER: The cluster (in a multi-cluster warehouse) that this statement executed on.
        COMPILATION_TIME_SEC: Compilation time (in seconds).
        CREDITS: Credits consumed by the query.
        EXECUTION_STATUS: Execution status for the query. Valid values: success, fail, incident.

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

        SELECT QH.QUERY_ID
            ,QH.QUERY_TYPE
            ,QH.QUERY_TEXT
            ,QH.USER_NAME
            ,QH.ROLE_NAME
            ,QH.DATABASE_NAME
            ,QH.SCHEMA_NAME
            ,QH.WAREHOUSE_NAME
            ,QH.WAREHOUSE_SIZE
            ,QH.BYTES_SPILLED_TO_LOCAL_STORAGE
            ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
            ,QH.PARTITIONS_SCANNED
            ,QH.PARTITIONS_TOTAL
            ,ROUND((QH.COMPILATION_TIME/(1000)),2) AS COMPILATION_TIME_SEC
            ,ROUND((QH.EXECUTION_TIME/(1000*60)),2) AS EXECUTION_TIME_MIN
            ,ROUND((QH.EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS
            ,QH.CLUSTER_NUMBER
            ,QH.EXECUTION_STATUS
            ,BYTES_SCANNED
            ,PERCENTAGE_SCANNED_FROM_CACHE*100 as percent_scanned_from_cache
        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        WHERE TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
        AND BYTES_SCANNED > 0
        ORDER BY PERCENT_SCANNED_FROM_CACHE DESC
        LIMIT {n}
       
        """
       
        
        df=self.query_to_df(sql)
        return df
    
    ##TODO: Convert this into N most frequently executed Select queries so these can be identified 
    # as targets for creating new tables or materialized views
    
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

    def longest_running_queries(self, start_date='2022-01-01',end_date='', n=10):
        """
        Shows the queries with the most elapsed time.
        Outputs a dataframe with the following columns:
        QUERY_TEXT: Text of SQL statement.
        QUERY_TYPE:DML, query, etc. If the query failed, then the query type may be UNKNOWN.
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
           SELECT QH.QUERY_ID
            ,QH.QUERY_TYPE
            ,QH.QUERY_TEXT
            ,QH.USER_NAME
            ,QH.ROLE_NAME
            ,QH.DATABASE_NAME
     BYTES_SCANNED: Number of bytes scanned by this statement.       ,QH.SCHEMA_NAME
            ,QH.WAREHOUSE_NAME
            ,QH.WAREHOUSE_SIZE
            ,QH.BYTES_SPILLED_TO_LOCAL_STORAGE
            ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
            ,QH.PARTITIONS_SCANNED
            ,QH.PARTITIONS_TOTAL
            ,ROUND((QH.COMPILATION_TIME/(1000)),2) AS COMPILATION_TIME_SEC
            ,ROUND((QH.EXECUTION_TIME/(1000*60)),2) AS EXECUTION_TIME_MIN
            ,ROUND((QH.EXECUTION_TIME/(1000*60*60))*WS.NODES,2) as CREDITS
            ,QH.CLUSTER_NUMBER
            ,QH.EXECUTION_STATUS

        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        where 1=1
        and TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
            and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
            and ERROR_CODE iS NULL
            and PARTITIONS_SCANNED is not null
        
        order by  TOTAL_ELAPSED_TIME desc
        
        LIMIT {n}
        """

        df=self.query_to_df(sql)
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

