class SNFLKQuery():
    def __init__(self, connection, dbname):
        self.connection = connection
        self.dbname = dbname
        
    def query_to_df(self, sql):
        return self.connection.cursor().execute(sql).fetch_pandas_all()
        
    def cost_by_usage(self, start_date, end_date):
        sql = f"""
            -- Calculating cost by usage type YTD
                WITH CONTRACT_VALUES AS (

                    SELECT
                            1.00::decimal(10,2) as CREDIT_PRICE
                            ,10000.00::decimal(38,0) as TOTAL_CONTRACT_VALUE
                            ,'2022-01-01'::timestamp as CONTRACT_START_DATE
                            ,DATEADD(month,12,'2022-01-01')::timestamp as CONTRACT_END_DATE

                ),
                PROJECTED_USAGE AS (

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
                --COST.WAREHOUSE_GROUP_NAME, COST.USER_NAME, COST.WAREHOUSE_NAME, COST.START_TIME, COST.END_TIME, SUM(COST.CREDITS_USED), SUM(CREDIT_PRICE), SUM(DOLLARS_USED) 
                SELECT DISTINCT cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as total_credits_used, SUM(cost.DOLLARS_USED) as total_dollars_used from (
                SELECT DISTINCT
                        'WH Compute' as WAREHOUSE_GROUP_NAME,
                        WEH.USER_NAME
                        ,WMH.WAREHOUSE_NAME
                        ,WMH.START_TIME
                        ,WMH.END_TIME
                        ,WMH.CREDITS_USED_COMPUTE as CREDITS_USED
                        ,1.00 as CREDIT_PRICE
                        ,(1.00*WMH.CREDITS_USED_COMPUTE) AS DOLLARS_USED
                        ,'ACTUAL COMPUTE' AS MEASURE_TYPE                   
                from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join KIV.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
                UNION
                --COMPUTE FROM SNOWPIPE
                SELECT
                        'Snowpipe' AS WAREHOUSE_GROUP_NAME,
                        'USER' as user_name
                        ,PUH.PIPE_NAME AS WAREHOUSE_NAME
                        ,PUH.START_TIME
                        ,PUH.END_TIME
                        ,PUH.CREDITS_USED
                        ,1.00 as CREDIT_PRICE
                        ,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
                        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
                from    KIV.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH

                UNION

                --COMPUTE FROM CLUSTERING
                SELECT
                        'Auto Clustering' AS WAREHOUSE_GROUP_NAME,
                        'USER' as user_name
                        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                        ,ACH.START_TIME
                        ,ACH.END_TIME
                        ,ACH.CREDITS_USED
                        ,1.00 as CREDIT_PRICE
                        ,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
                        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
                from    KIV.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH

                UNION

                --COMPUTE FROM MATERIALIZED VIEWS
                SELECT
                        'Materialized Views' AS WAREHOUSE_GROUP_NAME,
                        'USER' AS user_name
                        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                        ,MVH.START_TIME
                        ,MVH.END_TIME
                        ,MVH.CREDITS_USED
                        ,1.00 as CREDIT_PRICE
                        ,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
                        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
                from    KIV.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
                UNION
                SELECT
                        'Replication' AS WAREHOUSE_GROUP_NAME,
                        'USER' as user_name
                        ,DATABASE_NAME AS WAREHOUSE_NAME
                        ,RUH.START_TIME
                        ,RUH.END_TIME
                        ,RUH.CREDITS_USED
                        ,1.00 as CREDIT_PRICE
                        ,(1.00*RUH.CREDITS_USED) AS DOLLARS_USED
                        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
                from    KIV.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH

                UNION

                --STORAGE COSTS
                SELECT
                        'Storage' AS WAREHOUSE_GROUP_NAME,
                        'USER' as user_name
                        ,'Storage' AS WAREHOUSE_NAME
                        ,SU.USAGE_DATE
                        ,SU.USAGE_DATE
                        ,0 AS CREDITS_USED
                        ,1.00 as CREDIT_PRICE
                        ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
                        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
                from    KIV.ACCOUNT_USAGE.STORAGE_USAGE SU
                JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
                ) as COST group by 1 order by 2, 3 desc
                ;        
            """
        return self.query_to_df(sql)
    
    def cost_by_user_ts(self, start_date, end_date):
        sql = f"""
            SELECT DISTINCT cost.USER_NAME, cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_consumed, TIMESTAMPDIFF('hour', cost.START_TIME, cost.END_TIME) as duration, cost.START_TIME  from (
            SELECT DISTINCT
                    'WH Compute' as WAREHOUSE_GROUP_NAME,
                    WEH.USER_NAME
                    ,WMH.WAREHOUSE_NAME
                    ,WMH.START_TIME
                    ,WMH.END_TIME
                    ,WMH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*WMH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    KIV.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join KIV.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
            UNION
            --COMPUTE FROM SNOWPIPE
            SELECT
                    'Snowpipe' AS WAREHOUSE_GROUP_NAME,
                    'USER' as user_name
                    ,PUH.PIPE_NAME AS WAREHOUSE_NAME
                    ,PUH.START_TIME
                    ,PUH.END_TIME
                    ,PUH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    KIV.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH

            UNION

            --COMPUTE FROM CLUSTERING
            SELECT
                    'Auto Clustering' AS WAREHOUSE_GROUP_NAME,
                    'USER' as user_name
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,ACH.START_TIME
                    ,ACH.END_TIME
                    ,ACH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    KIV.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH

            UNION

            --COMPUTE FROM MATERIALIZED VIEWS
            SELECT
                    'Materialized Views' AS WAREHOUSE_GROUP_NAME,
                    'USER' AS user_name
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,MVH.START_TIME
                    ,MVH.END_TIME
                    ,MVH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    KIV.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
            UNION
            SELECT
                    'Replication' AS WAREHOUSE_GROUP_NAME,
                    'USER' as user_name
                    ,DATABASE_NAME AS WAREHOUSE_NAME
                    ,RUH.START_TIME
                    ,RUH.END_TIME
                    ,RUH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*RUH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    KIV.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH

            UNION

            --STORAGE COSTS
            SELECT
                    'Storage' AS WAREHOUSE_GROUP_NAME,
                    'USER' as user_name
                    ,'Storage' AS WAREHOUSE_NAME
                    ,SU.USAGE_DATE
                    ,SU.USAGE_DATE
                    ,NULL AS CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    KIV.ACCOUNT_USAGE.STORAGE_USAGE SU
            JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
            ) as COST group by 5, 1, 2, 4 order by 5 asc
            ;
        """
        return self.query_to_df(sql)
    
    