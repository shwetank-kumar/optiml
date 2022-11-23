class SNFLKQuery():
    def __init__(self, connection, dbname):
        self.connection = connection
        self.dbname = dbname

    def query_to_df(self, sql):
        return self.connection.cursor().execute(sql).fetch_pandas_all()

    def cost_by_usage_type(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where cost.start_time>='{}' and cost.end_time<='{}'".format(start_date, end_date)
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
                from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
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
                from    {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH

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
                from    {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH

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
                from    {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
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
                from    {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH

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
                from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
                JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
                ) as COST {ini_date} group by 1 order by 2, 3 desc
                ;        
            """
        return self.query_to_df(sql)

    def cost_by_user_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where cost.start_time>='{}' and cost.end_time<='{}'".format(start_date, end_date)
        sql = f"""
            --COST.WAREHOUSE_GROUP_NAME, COST.USER_NAME, COST.WAREHOUSE_NAME, COST.START_TIME, COST.END_TIME, SUM(COST.CREDITS_USED), SUM(CREDIT_PRICE), SUM(DOLLARS_USED)
            SELECT DISTINCT cost.USER_NAME, cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_used,
            sum(SUM(credits_used)) OVER (order by cost.START_TIME ASC) as Cumulative_Credits_Total, cost.START_TIME, cost.END_TIME  from (
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
            from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
            UNION
            --COMPUTE FROM SNOWPIPE
            SELECT
                     'Snowpipe' AS WAREHOUSE_GROUP_NAME,
                     'SNOWFLAKE' as user_name
                    ,PUH.PIPE_NAME AS WAREHOUSE_NAME
                    ,PUH.START_TIME
                    ,PUH.END_TIME
                    ,PUH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH

            UNION

            --COMPUTE FROM CLUSTERING
            SELECT
                     'Auto Clustering' AS WAREHOUSE_GROUP_NAME,
                     'SNOWFLAKE' as user_name
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,ACH.START_TIME
                    ,ACH.END_TIME
                    ,ACH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH

            UNION

            --COMPUTE FROM MATERIALIZED VIEWS
            SELECT
                     'Materialized Views' AS WAREHOUSE_GROUP_NAME,
                     'SNOWFLAKE' AS user_name
                    ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                    ,MVH.START_TIME
                    ,MVH.END_TIME
                    ,MVH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
            UNION
            SELECT
                     'Replication' AS WAREHOUSE_GROUP_NAME,
                     'SNOWFLAKE' as user_name
                    ,DATABASE_NAME AS WAREHOUSE_NAME
                    ,RUH.START_TIME
                    ,RUH.END_TIME
                    ,RUH.CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,(1.00*RUH.CREDITS_USED) AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH

            UNION

            --STORAGE COSTS
            SELECT
                     'Storage' AS WAREHOUSE_GROUP_NAME,
                     'SNOWFLAKE' as user_name
                    ,'Storage' AS WAREHOUSE_NAME
                    ,SU.USAGE_DATE
                    ,SU.USAGE_DATE
                    ,NULL AS CREDITS_USED
                    ,1.00 as CREDIT_PRICE
                    ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
                    ,'ACTUAL COMPUTE' AS MEASURE_TYPE
            from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
            JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
            ) as COST {ini_date} group by 5, 1, 2, 6 order by 5 asc
            ;

        """
        return self.query_to_df(sql)

    def cost_by_wh_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where cost.start_time>='{}' and cost.end_time<='{}'".format(start_date, end_date)
        sql = f"""
                --COST.WAREHOUSE_GROUP_NAME, COST.USER_NAME, COST.WAREHOUSE_NAME, COST.START_TIME, COST.END_TIME, SUM(COST.CREDITS_USED), SUM(CREDIT_PRICE), SUM(DOLLARS_USED)
        SELECT DISTINCT cost.WAREHOUSE_NAME, cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_used, sum(SUM(credits_used)) OVER (order by cost.START_TIME ASC) as Cumulative_Credits_Total, cost.START_TIME, cost.END_TIME  from (
        SELECT DISTINCT
                 'WH Compute' as WAREHOUSE_GROUP_NAME,
                 WEH.USER_NAME
                ,WMH.WAREHOUSE_NAME
                ,WMH.START_TIME
                ,WMH.END_TIME
                ,WMH.CREDITS_USED
                --,1.00 as CREDIT_PRICE
                ,NULL AS DOLLARS_USED
                ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
        UNION
        --COMPUTE FROM SNOWPIPE
        SELECT
                 'Snowpipe' AS WAREHOUSE_GROUP_NAME,
                 'SNOWFLAKE' as user_name
                ,PUH.PIPE_NAME AS WAREHOUSE_NAME
                ,PUH.START_TIME
                ,PUH.END_TIME
                ,PUH.CREDITS_USED
                --,1.00 as CREDIT_PRICE
                ,NULL AS DOLLARS_USED
                ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH

        UNION

        --COMPUTE FROM CLUSTERING
        SELECT
                 'Auto Clustering' AS WAREHOUSE_GROUP_NAME,
                 'SNOWFLAKE' as user_name
                ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                ,ACH.START_TIME
                ,ACH.END_TIME
                ,ACH.CREDITS_USED
                --,1.00 as CREDIT_PRICE
                ,NULL AS DOLLARS_USED
                ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH

        UNION

        --COMPUTE FROM MATERIALIZED VIEWS
        SELECT
                 'Materialized Views' AS WAREHOUSE_GROUP_NAME,
                 'SNOWFLAKE' AS user_name
                ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
                ,MVH.START_TIME
                ,MVH.END_TIME
                ,MVH.CREDITS_USED
                --,1.00 as CREDIT_PRICE
                ,NULL AS DOLLARS_USED
                ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
        UNION
        SELECT
                 'Replication' AS WAREHOUSE_GROUP_NAME,
                 'SNOWFLAKE' as user_name
                ,DATABASE_NAME AS WAREHOUSE_NAME
                ,RUH.START_TIME
                ,RUH.END_TIME
                ,RUH.CREDITS_USED
                --,1.00 as CREDIT_PRICE
                ,NULL AS DOLLARS_USED
                ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH

        UNION

        --STORAGE COSTS
        SELECT
                 'Storage' AS WAREHOUSE_GROUP_NAME,
                 'SNOWFLAKE' as user_name
                ,'Storage' AS WAREHOUSE_NAME
                ,SU.USAGE_DATE
                ,SU.USAGE_DATE
                ,NULL AS CREDITS_USED
                --,1.00 as CREDIT_PRICE
                ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
                ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
        JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
        ) as COST {ini_date} group by 5, 1, 2, 6 order by 5 asc
        ;

        """

        return self.query_to_df(sql)

    def cost_of_autoclustering_ts(self, start_date, end_date):
        """Calculates the overall cost of autoclustering for a given time period using Automatic 
        Clustering History table. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Database name: The database name of the table that was clustered
        Schema name: The schema name of the table that was clustered
        Table name: The name of the table that was clustered
        Total credits used: Total credits used during the billing period
        """
        ini_date = ""
        if start_date and end_date:
            ini_date = "where ACH.start_time>='{}' and ACH.end_time<='{}'".format(start_date, end_date)
        sql = f"""
        SELECT
        'Auto Clustering' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,sum(ACH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by ACH.START_TIME ASC) as Cumulative_Credits_Total
        ,ACH.START_TIME
        ,ACH.END_TIME
        --,1.00 as CREDIT_PRICE
        --,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
        --,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH {ini_date} group by 5, 2, 6 order by 5;
        """

        # Use this:
        # select to_date(start_time) as date
        # ,database_name
        # ,schema_name
        # ,table_name
        # ,sum(credits_used) as credits_used

        # from "SNOWFLAKE"."ACCOUNT_USAGE"."AUTOMATIC_CLUSTERING_HISTORY"
        
        # where start_time >= dateadd(month,-1,current_timestamp())
        # group by 1,2,3,4
        # order by 5 desc
        # ;

        return self.query_to_df(sql)

    def cost_of_cloud_services_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where cost.start_time>='{}' and cost.end_time<='{}'".format(start_date, end_date)
        sql = f"""
        SELECT DISTINCT cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_used, 
        sum(SUM(credits_used)) OVER (order by cost.START_TIME ASC) as Cumulative_Credits_Total, cost.START_TIME, cost.END_TIME  from (
        SELECT DISTINCT
         'WH Compute' as WAREHOUSE_GROUP_NAME,
         WEH.USER_NAME
        ,WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,WMH.END_TIME
        ,WMH.CREDITS_USED_CLOUD_SERVICES as CREDITS_USED
        --,1.00 as CREDIT_PRICE
        --,(1.00*WMH.CREDITS_USED_CLOUD_SERVICES) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
         ) as COST {ini_date} group by 4, 1, 5 order by 4 asc;
        """

        return self.query_to_df(sql)

    def cost_of_compute_ts(self, start_date, end_date):
        """Calculates the overall cost of compute for a given time period using Snowflake Warehouse 
        Metering History tables. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Total credits used: Total credits used during the billing period
        Compute credits used: Compute credits used during the billing period
        Cloud services credits used: Cloud services credits used to support the compute
        """
        ini_date = ""
        if start_date and end_date:
            ini_date = "where cost.start_time>='{}' and cost.end_time<='{}'".format(start_date, end_date)
        sql = f"""
        SELECT DISTINCT cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as total_credits_used,
        sum(SUM(credits_used)) OVER (order by cost.START_TIME ASC) as Cumulative_Credits_Total, cost.start_time, cost.end_time from (
        SELECT DISTINCT
         'WH Compute' as WAREHOUSE_GROUP_NAME,
         WEH.USER_NAME
        ,WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,WMH.END_TIME
        ,WMH.CREDITS_USED_COMPUTE as CREDITS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
        inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH
        on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
        ) as COST {ini_date} group by 4, 1, 5 order by 4 ASC;
        """

        return self.query_to_df(sql)

    def cost_of_materialized_views_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where MVH.start_time>='{}' and MVH.end_time<='{}'".format(start_date, end_date)
        sql = f"""
        SELECT
         'Materialized Views' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,SUM(MVH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by MVH.START_TIME ASC) as Cumulative_Credits_Total
        ,MVH.START_TIME
        ,MVH.END_TIME
        --,1.00 as CREDIT_PRICE
        --,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
        --,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH {ini_date} group by 5, 2, 6 order by 5;
        """

        return self.query_to_df(sql)

    def cost_of_replication_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where RUH.start_time>='{}' and RUH.end_time<='{}'".format(start_date, end_date)
        sql = f"""
          SELECT
         'Replication' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME AS WAREHOUSE_NAME
        ,SUM(RUH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by RUH.START_TIME ASC) as Cumulative_Credits_Total
        ,RUH.START_TIME
        ,RUH.END_TIME
        from    {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH {ini_date} group by 5, 2, 6 order by 5;
        """

        return self.query_to_df(sql)

    def cost_of_searchoptimization_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where SOH.start_time>='{}' and SOH.end_time<='{}'".format(start_date, end_date)
        sql = f"""
         SELECT
         'Search Optimization' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,SUM(SOH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by SOH.START_TIME ASC) as Cumulative_Credits_Total
        ,SOH.START_TIME
        ,SOH.END_TIME
        from    {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY SOH {ini_date} group by 5, 2, 6 order by 5;
        """

        return self.query_to_df(sql)

    def cost_of_snowpipe_ts(self, start_date, end_date):
        ini_date = ""
        if start_date and end_date:
            ini_date = "where PUH.start_time>='{}' and PUH.end_time<='{}'".format(start_date, end_date)
        sql = f"""
          SELECT
         'Snowpipe' AS WAREHOUSE_GROUP_NAME
        ,PUH.PIPE_NAME AS WAREHOUSE_NAME
        ,SUM(PUH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by PUH.START_TIME ASC) as Cumulative_Credits_Total
        ,PUH.START_TIME
        ,PUH.END_TIME
        --,1.00 as CREDIT_PRICE
        --,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
        --,'ACTUAL COMPUTE' AS MEASURE_TYPE
        from    {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH {ini_date} group by 5, 2, 6 order by 5;
        """

        return self.query_to_df(sql)

    def cost_of_storage_ts(self):
        sql = f"""
         select cost.WAREHOUSE_NAME, cost.USAGE_DATE, cost.DOLLARS_USED, sum(SUM(DOLLARS_USED)) OVER (order by cost.USAGE_DATE ASC) as Cumulative_Credits_Total from (
         SELECT
        'Storage' AS WAREHOUSE_NAME
        ,SU.USAGE_DATE
        ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
        from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
        JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH
        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM
        GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA
        ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')) as cost group by 1, 2, 3 order by 2;
        """

        return self.query_to_df(sql)

    def show_wh_without_autosuspend(self):
        sql = f"""
        SHOW WAREHOUSES;
        select * from table(result_scan(last_query_id())) where "auto_suspend" is NULL;
        """

        return self.query_to_df(sql)

    def show_wh_without_resource_monitors(self):
        sql = f"""
        SHOW WAREHOUSES;
        select * from table(result_scan(last_query_id())) where "resource_monitor"='null';
        """

        return self.query_to_df(sql)