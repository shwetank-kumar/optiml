import logging as log
from   textwrap                 import dedent

import pandas as pd


def cursor_to_df(cursor, lowercase_colnames=True):
    # Note: Prefer to use fetch_pandas_all over using this function
    if hasattr(cursor, "fetch_pandas_all"):
        df = cursor.fetch_pandas_all()
        if lowercase_colnames:
            df.columns = map(str.lower, df.columns)  # PG does now like upper case col names
    else:
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        if lowercase_colnames:
            column_names = [col.lower() for col in column_names]
        df = pd.DataFrame(rows, columns=column_names)
    return df


def canonicalize_intervals_df(df, start_time_colname="start_time", end_time_colname="end_time"):
    '''
    take a df with end_time and start_time columns and:
     - add a interval_duration_sec column (diff between the two above)
     - set start_time as the sorted index
    '''
    if df.shape[0] > 0:
        df['interval_duration_sec'] = (df[end_time_colname] - df[start_time_colname]).dt.seconds
    else:
        df['interval_duration_sec'] = 0.0
    df.set_index(start_time_colname, inplace=True)
    df.sort_index(inplace=True)
    return df


SNFLK_EDITION_TO_COST_PER_CREDIT = dict(
    # Mapping from SNFLK edition name to cost per 1 credit in USD,
    standard=2.0,
    enterprise=3.0,
)


class SqlQueryBase:
    def __init__(self):
        pass



class SnflkQueryLib:
    '''
    A library of functions that essentially wrap useful queries.
    
    example usage:
    
        conn = SnowflakeConnConfig(accountname='MNVZTWX-AVIV_GCP',
                                  rolename="KNT_ADMIN",
                                  warehousename="XSMALL_WH",
                                 ).create_connection() 
        qlib = SnflkQueryLib(conn=conn, dbname="KNT")
        c = qlib.get_avg_cost_per_wh(start_date='2022-10-10', end_date='2022-10-21')
        cursor_to_df(c)
    '''

    def __init__(self,
                 conn,
                 dbname,
                 snlfk_edition="enterprise",
                 storage_usd_per_tb_month=23):
        self.conn = conn
        self.dbname = dbname
        assert snlfk_edition in SNFLK_EDITION_TO_COST_PER_CREDIT.keys()
        self.snflk_edition = snlfk_edition
        self.credit_price_usd = SNFLK_EDITION_TO_COST_PER_CREDIT[snlfk_edition]
        self.storage_usd_per_tb_month = int(storage_usd_per_tb_month)


    def _exec_sql(self, sql):
        log.info(f"Executing SQL: {sql}")
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor


    def get_avg_cost_per_wh(self, start_date, end_date):
        sql = dedent(
            f'''
            SELECT
                COALESCE(WC.WAREHOUSE_NAME,QC.WAREHOUSE_NAME) AS WAREHOUSE_NAME
                ,QC.QUERY_COUNT_LAST_MONTH
                ,WC.CREDITS_USED_LAST_MONTH
                ,WC.CREDIT_COST_LAST_MONTH
                ,CAST((WC.CREDIT_COST_LAST_MONTH / QC.QUERY_COUNT_LAST_MONTH) AS decimal(10,2) ) AS COST_PER_QUERY
            
            FROM (
                SELECT
                   WAREHOUSE_NAME
                  ,COUNT(QUERY_ID) as QUERY_COUNT_LAST_MONTH
                FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE TO_DATE(START_TIME) >= TO_DATE('{start_date}') and TO_DATE(START_TIME) <= TO_DATE('{end_date}')
                GROUP BY WAREHOUSE_NAME
                  ) QC
            JOIN (
            
                SELECT
                    WAREHOUSE_NAME
                    ,SUM(CREDITS_USED) as CREDITS_USED_LAST_MONTH
                    ,SUM(CREDITS_USED)* {self.credit_price_usd} as CREDIT_COST_LAST_MONTH
                FROM {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE TO_DATE(START_TIME) >= TO_DATE('{start_date}') and TO_DATE(START_TIME) <= TO_DATE('{end_date}')
                GROUP BY WAREHOUSE_NAME
              ) WC
                ON WC.WAREHOUSE_NAME = QC.WAREHOUSE_NAME
            
            ORDER BY COST_PER_QUERY DESC
            '''
        )
        return self._exec_sql(sql)


    def get_combined_billing(self, term_start_date, term_length_months, term_value_usd):
        '''
        Billing Metric (T1)
        Identify key metrics as it pertains to total compute costs from warehouses, serverless features, and total storage costs.

        How to Interpret Results:
        Where are we seeing most of our costs coming from (compute, serverless, storage)?
        Are seeing excessive costs in any of those categories that are above expectations?

        /* These queries can be used to measure where costs have been incurred by
           the different cost vectors within a Snowflake account including:
           1) Warehouse Costs
           2) Serverless Costs
           3) Storage Costs


        */

        https: // quickstarts.snowflake.com / guide / resource_optimization_billing_metrics / index.html?index =..% 2F..index#2

        '''
        sql = dedent(f'''
            WITH CONTRACT_VALUES AS (            
                  SELECT
                           {self.credit_price_usd}::decimal(10,2) as CREDIT_PRICE
                          ,{term_value_usd}::decimal(38,0) as TOTAL_CONTRACT_VALUE
                          ,'{term_start_date}'::timestamp as CONTRACT_START_DATE
                          ,DATEADD(month,{term_length_months},'{term_start_date}')::timestamp as CONTRACT_END_DATE
            
            ),
            PROJECTED_USAGE AS (
                  -- derive DOLLARS_PER_DAY and CREDITS_PER_DAY
                  SELECT
                             CREDIT_PRICE
                            ,TOTAL_CONTRACT_VALUE
                            ,CONTRACT_START_DATE
                            ,CONTRACT_END_DATE
                            ,(TOTAL_CONTRACT_VALUE)
                                /
                                DATEDIFF(day,CONTRACT_START_DATE,CONTRACT_END_DATE)  AS DOLLARS_PER_DAY
                            , (TOTAL_CONTRACT_VALUE/CREDIT_PRICE)
                                /
                                DATEDIFF(day,CONTRACT_START_DATE,CONTRACT_END_DATE) AS CREDITS_PER_DAY
                  FROM      CONTRACT_VALUES
            
            )
            
            --COMPUTE FROM WAREHOUSE_METERING_HISTORY 
            SELECT
                     'WH Compute' as WAREHOUSE_GROUP_NAME
                    ,WMH.WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,WMH.START_TIME
                    ,WMH.END_TIME
                    ,WMH.CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,({self.credit_price_usd} * WMH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE                   
            from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
            
            UNION ALL
            
            --COMPUTE FROM PIPE_USAGE_HISTORY PUH
            SELECT
                     'Snowpipe' AS WAREHOUSE_GROUP_NAME
                    ,PUH.PIPE_NAME AS WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,PUH.START_TIME
                    ,PUH.END_TIME
                    ,PUH.CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,({self.credit_price_usd} * PUH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH
            
            UNION ALL
            
            --COMPUTE FROM AUTOMATIC_CLUSTERING_HISTORY
            SELECT
                     'Auto Clustering' AS WAREHOUSE_GROUP_NAME
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,ACH.START_TIME
                    ,ACH.END_TIME
                    ,ACH.CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,({self.credit_price_usd} * ACH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH
            
            UNION ALL
            
            --COMPUTE FROM MATERIALIZED_VIEW_REFRESH_HISTORY VIEWS
            SELECT
                     'Materialized Views' AS WAREHOUSE_GROUP_NAME
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,MVH.START_TIME
                    ,MVH.END_TIME
                    ,MVH.CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,({self.credit_price_usd} * MVH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
            
            UNION ALL
            
            --COMPUTE FROM SEARCH_OPTIMIZATION_HISTORY
            SELECT
                     'Search Optimization' AS WAREHOUSE_GROUP_NAME
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,SOH.START_TIME
                    ,SOH.END_TIME
                    ,SOH.CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,({self.credit_price_usd} * SOH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY SOH
            
            UNION ALL
            
            --COMPUTE FROM REPLICATION_USAGE_HISTORY
            SELECT
                     'Replication' AS WAREHOUSE_GROUP_NAME
                    ,DATABASE_NAME AS WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,RUH.START_TIME
                    ,RUH.END_TIME
                    ,RUH.CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,({self.credit_price_usd} * RUH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH
            
            UNION ALL
            
            --STORAGE STORAGE_USAGE
            SELECT
                     'Storage' AS WAREHOUSE_GROUP_NAME
                    ,'Storage' AS WAREHOUSE_NAME
                    ,NULL AS GROUP_CONTACT
                    ,NULL AS GROUP_COST_CENTER
                    ,NULL AS GROUP_COMMENT
                    ,SU.USAGE_DATE
                    ,SU.USAGE_DATE
                    ,NULL AS CREDITS_USED
                    ,{self.credit_price_usd} as CREDIT_PRICE
                    ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
            JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,
                            TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH 
                         FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM 
                         GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA 
                     ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
            
            UNION ALL
            
            
            SELECT
                     NULL as WAREHOUSE_GROUP_NAME
                    ,NULL as WAREHOUSE_NAME
                    ,NULL as GROUP_CONTACT
                    ,NULL as GROUP_COST_CENTER
                    ,NULL as GROUP_COMMENT
                    ,DA.D_DATE::timestamp as START_TIME
                    ,DA.D_DATE::timestamp as END_TIME
                    ,PU.CREDITS_PER_DAY AS CREDITS_USED
                    ,PU.CREDIT_PRICE
                    ,PU.DOLLARS_PER_DAY AS DOLLARS_USED
                    ,'PROJECTED COMPUTE' AS MEASURE_TYPE
            FROM    PROJECTED_USAGE PU
            JOIN    SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM DA ON DA.D_DATE BETWEEN PU.CONTRACT_START_DATE AND PU.CONTRACT_END_DATE
            
            UNION ALL
            
            
            SELECT
                     NULL as WAREHOUSE_GROUP_NAME
                    ,NULL as WAREHOUSE_NAME
                    ,NULL as GROUP_CONTACT
                    ,NULL as GROUP_COST_CENTER
                    ,NULL as GROUP_COMMENT
                    ,NULL as START_TIME
                    ,NULL as END_TIME
                    ,NULL AS CREDITS_USED
                    ,PU.CREDIT_PRICE
                    ,PU.TOTAL_CONTRACT_VALUE AS DOLLARS_USED
                    ,'CONTRACT VALUES' AS MEASURE_TYPE
            FROM    PROJECTED_USAGE PU
            ;                
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        return df


    def get_warehouse_load_history(self, start_date, end_date):
        sql = dedent(f'''
            select start_time, 
                    end_time, 
                    warehouse_id, 
                    warehouse_name, 
                    avg_running, 
                    avg_queued_load,  
                    avg_queued_provisioning, 
                    avg_blocked 
            from {self.dbname}.account_usage.warehouse_load_history 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df['duration_sec'] = (df['end_time'] - df['start_time']) / 1e9 # convert ns to sec
        df.set_index("start_time", inplace=True)
        df.sort_index(inplace=True)
        return df


    def get_warehouse_metering_history(self, start_date, end_date):
        sql = dedent(f'''
            select * 
            from {self.dbname}.account_usage.warehouse_metering_history 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df = canonicalize_intervals_df(df)
        df['dollar_used'] = df['credits_used'] * self.credit_price_usd
        return df


    def get_pipe_usage_history(self, start_date, end_date):
        sql = dedent(f'''
            select * 
            from {self.dbname}.account_usage.pipe_usage_history 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df = canonicalize_intervals_df(df)
        df['dollar_used'] = df['credits_used'] * self.credit_price_usd
        return df


    def get_automatic_clustering_history(self, start_date, end_date):
        sql = dedent(f'''
            select * 
            from {self.dbname}.account_usage.AUTOMATIC_CLUSTERING_HISTORY 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df = canonicalize_intervals_df(df)
        df['dollar_used'] = df['credits_used'] * self.credit_price_usd
        return df


    def get_materialized_view_refresh_history(self, start_date, end_date):
        sql = dedent(f'''
            select * 
            from {self.dbname}.account_usage.MATERIALIZED_VIEW_REFRESH_HISTORY 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df = canonicalize_intervals_df(df)
        df['dollar_used'] = df['credits_used'] * self.credit_price_usd
        return df


    def get_search_opt_history(self, start_date, end_date):
        sql = dedent(f'''
            select * 
            from {self.dbname}.account_usage.SEARCH_OPTIMIZATION_HISTORY 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df = canonicalize_intervals_df(df)
        df['dollar_used'] = df['credits_used'] * self.credit_price_usd
        return df


    def get_replication_history(self, start_date, end_date):
        sql = dedent(f'''
            select * 
            from {self.dbname}.account_usage.REPLICATION_USAGE_HISTORY 
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            order by start_time
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        df = canonicalize_intervals_df(df)
        df['dollar_used'] = df['credits_used'] * self.credit_price_usd
        return df


    def get_storage_history(self, start_date, end_date):
        TB = f"(1024*1024*1024*2014)"
        sql = dedent(f'''
            select usage_date, 
                   storage_bytes / {TB} as storage_tb,
                   stage_bytes / {TB} as stage_tb,
                   failsafe_bytes / {TB} as failsafe_tb,
                   {self.storage_usd_per_tb_month} * storage_tb / DA.DAYS_IN_MONTH   as storage_cost_usd,
                   {self.storage_usd_per_tb_month} * stage_tb / DA.DAYS_IN_MONTH     as stage_cost_usd,
                   {self.storage_usd_per_tb_month} * failsafe_tb / DA.DAYS_IN_MONTH as failsafe_cost_usd,
                   storage_cost_usd + stage_cost_usd + failsafe_cost_usd as tot_storage_cost_usd,
                   DA.DAYS_IN_MONTH as days_in_month
            from {self.dbname}.account_usage.STORAGE_USAGE as SU
            JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH, TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH
                     FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM 
                     GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA 
            ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
            where usage_date >= '{start_date}' and usage_date <= '{end_date}'
            order by usage_date            
        ''')
        df = cursor_to_df(self._exec_sql(sql))

        return df
    
    def get_n_most_expensive_queries(self, start_date, end_date, n=10):
        sql = dedent(f'''
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
                    ,QH.EXECUTION_TIME
                    ,QH.WAREHOUSE_SIZE
                FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
                where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
            )

            SELECT QH.QUERY_ID
                ,'https://' || current_account() || '.snowflakecomputing.com/console#/monitoring/queries/detail?queryId='||QH.QUERY_ID AS QU
                ,QH.QUERY_TEXT
                ,QH.USER_NAME
                ,QH.ROLE_NAME
                ,QH.EXECUTION_TIME as EXECUTION_TIME_MILLISECONDS
                ,(QH.EXECUTION_TIME/(1000)) as EXECUTION_TIME_SECONDS
                ,(QH.EXECUTION_TIME/(1000*60)) AS EXECUTION_TIME_MINUTES
                ,(QH.EXECUTION_TIME/(1000*60*60)) AS EXECUTION_TIME_HOURS
                ,WS.WAREHOUSE_SIZE
                ,WS.NODES
                ,(QH.EXECUTION_TIME/(1000*60*60))*WS.NODES as RELATIVE_PERFORMANCE_COST

            FROM QUERY_HISTORY QH
            JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
            ORDER BY RELATIVE_PERFORMANCE_COST DESC
            LIMIT {n}
            ;
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        return df

    def get_n_longest_queries(self, start_date, end_date, n=10):
        sql = dedent(f'''
            select QUERY_ID
                --reconfigure the url if your account is not in AWS US-West
                ,'https://'||CURRENT_ACCOUNT()||'.snowflakecomputing.com/console#/monitoring/queries/detail?queryId='||Q.QUERY_ID as QUERY_PROFILE_URL
                ,ROW_NUMBER() OVER(ORDER BY PARTITIONS_SCANNED DESC) as QUERY_ID_INT
                ,QUERY_TEXT
                ,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
                ,PARTITIONS_SCANNED
                ,PARTITIONS_TOTAL
            from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY Q
                where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
                    and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
                    and ERROR_CODE iS NULL
                    and PARTITIONS_SCANNED is not null
                
                order by  TOTAL_ELAPSED_TIME desc
                
                LIMIT {n}
                ;
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        return df
    
    def get_n_queries_most_scanning_queries(self, start_date, end_date, n=10):
        sql = dedent(f'''
            select
          
                QUERY_ID
                --reconfigure the url if your account is not in AWS US-West
                ,'https://'||CURRENT_ACCOUNT()||'.snowflakecomputing.com/console#/monitoring/queries/detail?queryId='||Q.QUERY_ID as QUERY_PROFILE_URL
                ,ROW_NUMBER() OVER(ORDER BY PARTITIONS_SCANNED DESC) as QUERY_ID_INT
                ,QUERY_TEXT
                ,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
                ,PARTITIONS_SCANNED
                ,PARTITIONS_TOTAL

            from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY Q
            where start_time::date >= '{start_date}' and end_time::date <= '{end_date}'
                and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
                and ERROR_CODE iS NULL
                and PARTITIONS_SCANNED is not null
            
            order by  PARTITIONS_SCANNED desc
            
            LIMIT 50
            ;
            
        ''')
        df = cursor_to_df(self._exec_sql(sql))
        return df