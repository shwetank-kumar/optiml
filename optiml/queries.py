from datetime import date


class SNFLKQuery():
    credit_values = {
    "standard": 2,
    "enterprise": 3,
    "business critical": 4
    }
    def __init__(self, connection, dbname, credit_value="standard"):
        self.connection = connection
        self.dbname = dbname
        self.credit_value = credit_value

    def query_to_df(self, sql):
        return self.connection.cursor().execute(sql).fetch_pandas_all()

    def total_cost_breakdown(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        #TODO Write funcion
        """Compute	Credits and $
            Storage	Null and $
            Cloud Services	Credits and $
            Autoclustering	Credits and $
            Materialized view	Credits and $
            Search Optimization 	Credits and $
            Snowpipe usage	Credits and $
            Replication	Credits and $"""
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
            SELECT
                'STORAGE' AS COST_CATEGORY
                ,NULL as TOTAL_CREDITS
                ,IFNULL(sum(((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH), NULL) AS TOTAL_DOLLARS_USED
            from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
            JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH
            FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM
            GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA
            ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
            where SU.USAGE_DATE between '{start_date}' and '{end_date}' group by 1

            UNION

            SELECT DISTINCT
                    'COMPUTE' as COST_CATEGORY
                    ,IFNULL(sum(WMH.CREDITS_USED_COMPUTE), NULL) as TOTAL_CREDITS_USED
                    ,IFNULL(({credit_val}*TOTAL_CREDITS_USED), NULL) as TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
            where WMH.START_TIME between '{start_date}' and '{end_date}' group by 1

            UNION

            SELECT DISTINCT
                    'CLOUD SERVICES' AS COST_CATEGORY
                    ,IFNULL(SUM(WMH.CREDITS_USED_CLOUD_SERVICES), NULL) as TOTAL_CREDITS_USED
                    ,IFNULL(({credit_val}*TOTAL_CREDITS_USED), NULL) AS TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
            where WMH.START_TIME between '{start_date}' and '{end_date}' group by 1

            UNION

            select
                'AUTOCLUSTERING' AS COST_CATEGORY,
                IFNULL(sum(credits_used), NULL) AS TOTAL_CREDITS_USED,
                IFNULL(({credit_val}*total_credits_used), NULL) AS TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
            where start_time between '{start_date}' and '{end_date}'

            UNION

            SELECT
                'SNOWPIPE' AS COST_CATEGORY
                ,IFNULL(SUM(PUH.CREDITS_USED), NULL) as TOTAL_CREDITS_USED
                ,IFNULL(({credit_val}*TOTAL_CREDITS_USED), NULL) AS TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH
            where start_time between '{start_date}' and '{end_date}'

            UNION

            select
                'MATERIALIZED VIEW' AS COST_CATEGORY
                ,IFNULL(SUM(MVH.credits_used), NULL) AS TOTAL_CREDITS_USED
                ,IFNULL(({credit_val}*TOTAL_CREDITS_USED), NULL) AS TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
            where start_time between '{start_date}' and '{end_date}'

            UNION

            select
                'REPLICATION' AS COST_CATEGORY
                ,IFNULL(SUM(RUH.credits_used), NULL) AS TOTAL_CREDITS_USED
                ,IFNULL(({credit_val}*TOTAL_CREDITS_USED), NULL) AS TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH
            where start_time between '{start_date}' and '{end_date}'

            UNION

            select
                'SEARCH OPTIMIZATION HISTORY' AS COST_CATEGORY
                ,IFNULL(SUM(SOH.credits_used), NULL) AS TOTAL_CREDITS_USED
                ,IFNULL(({credit_val}*TOTAL_CREDITS_USED), NULL) AS TOTAL_DOLLARS_USED
            from {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY SOH
            where start_time between '{start_date}' and '{end_date}'
        """
        return self.query_to_df(sql)

    # def cost_by_usage(self, start_date='2022-01-01', end_date=''):
    #     ##TODO Update docstring
    #     ##TODO Why is this giving cost by warehouse use?
    #     if not end_date:
    #         today_date = date.today()
    #         end_date = str(today_date)
    #         print(end_date)
    #     credit_val = ''
    #     if self.credit_value:
    #         credit_val = SNFLKQuery.credit_values[self.credit_value]
    #     sql = f"""
    #         with cte_date_wh as (
    #           select
    #               warehouse_name
    #               ,sum(credits_used) as credits_used_date_wh
    #               ,start_time
    #               ,end_time
    #           from {self.dbname}.account_usage.warehouse_metering_history
    #           group by start_time, warehouse_name, end_time
    #         )
    #         select
    #               warehouse_name
    #               ,sum(credits_used_date_wh) as total_credits_used
    #               ,({credit_val}*total_credits_used) as total_dollars_used
    #         from cte_date_wh where start_time between '{start_date}' and '{end_date}' group by warehouse_name;
    #         """
    #     return self.query_to_df(sql)

    # def cost_by_usage_ts(self, start_date='2022-01-01', end_date=''):
    #     ##TODO Update docstring
    #     if not end_date:
    #         today_date = date.today()
    #         end_date = str(today_date)
    #     credit_val = ''
    #     if self.credit_value:
    #         credit_val = SNFLKQuery.credit_values[self.credit_value]
    #     sql = f"""
    #         with cte_date_wh as(
    #           select
    #               warehouse_name
    #               ,sum(credits_used) as credits_used_date_wh
    #               ,start_time
    #               ,end_time
    #           from {self.dbname}.account_usage.warehouse_metering_history
    #           group by start_time,warehouse_name,end_time
    #         )
    #         select
    #               warehouse_name
    #               ,credits_used_date_wh
    #               ,({credit_val}*credits_used_date_wh) as total_dollars_used
    #               ,start_time
    #               ,end_time
    #         from cte_date_wh where start_time between '{start_date}' and '{end_date}' order by start_time;
    #         """
    #     return self.query_to_df(sql)

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

    def cost_by_wh_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
                select warehouse_name
                      ,credits_used
                      ,({credit_val}*credits_used) as total_dollars_used
                      ,start_time
                      ,end_time
                from {self.dbname}.account_usage.warehouse_metering_history
                where start_time between '{start_date}' and '{end_date}' -->= dateadd(day, -5, current_timestamp())
                group by 1,2,4,5
                order by 4 asc;
        """
        return self.query_to_df(sql)

    def cost_by_wh(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
                select warehouse_name
                      ,sum(credits_used) as credits_used_compute_sum
                      ,({credit_val}*credits_used_compute_sum) as total_dollars_used
                from {self.dbname}.account_usage.warehouse_metering_history
                where start_time between '{start_date}' and '{end_date}' -->= dateadd(day, -5, current_timestamp())
                group by 1
                order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_autoclustering_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of autoclustering for a given time period using Automatic
        Clustering History table. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Database name: The database name of the table that was clustered
        Schema name: The schema name of the table that was clustered
        Table name: The name of the table that was clustered
        Total credits used: Total credits used during the billing period
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        select database_name
              ,schema_name
              ,table_name
              ,sum(credits_used) as credits_used
              ,({credit_val}*credits_used) as total_dollars_used
              ,start_time
              ,end_time
        from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3,5,6,7
        order by 6 desc;
        """
        return self.query_to_df(sql)

    def cost_of_autoclustering(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of autoclustering for a given time period using Automatic
        Clustering History table. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Database name: The database name of the table that was clustered
        Schema name: The schema name of the table that was clustered
        Table name: The name of the table that was clustered
        Total credits used: Total credits used during the billing period
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        select database_name
              ,schema_name
              ,table_name
              ,sum(credits_used) as total_credits_used
              ,({credit_val}*total_credits_used) as total_dollars_used
        from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3
        order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_cloud_services_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        SELECT DISTINCT
                WMH.WAREHOUSE_NAME
                ,WMH.CREDITS_USED_CLOUD_SERVICES as CREDITS_USED
                ,({credit_val}*CREDITS_USED) as total_dollars_used
                ,WMH.START_TIME
                ,WMH.END_TIME
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;
        """

        return self.query_to_df(sql)

    def cost_of_cloud_services(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        SELECT DISTINCT
                WMH.WAREHOUSE_NAME
                ,SUM(WMH.CREDITS_USED_CLOUD_SERVICES) as TOTAL_CREDITS_USED
                ,({credit_val}*TOTAL_CREDITS_USED) as total_dollars_used
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
        where WMH.START_TIME between '{start_date}' and '{end_date}' group by 1 order by 1 asc
        """

        return self.query_to_df(sql)

    def cost_of_compute_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of compute for a given time period using Snowflake Warehouse
        Metering History tables. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Total credits used: Total credits used during the billing period
        Compute credits used: Compute credits used during the billing period
        Cloud services credits used: Cloud services credits used to support the compute
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        SELECT DISTINCT
                WMH.WAREHOUSE_NAME
                ,WMH.CREDITS_USED_COMPUTE as CREDITS_USED
                ,({credit_val}*CREDITS_USED) as total_dollars_used
                ,WMH.START_TIME
                ,WMH.END_TIME
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;
        """
        return self.query_to_df(sql)

    def cost_of_compute(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of compute for a given time period using Snowflake Warehouse
        Metering History tables. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Total credits used: Total credits used during the billing period
        Compute credits used: Compute credits used during the billing period
        Cloud services credits used: Cloud services credits used to support the compute
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        SELECT DISTINCT
                WMH.WAREHOUSE_NAME
                ,sum(WMH.CREDITS_USED_COMPUTE) as TOTAL_CREDITS_USED
                ,({credit_val}*TOTAL_CREDITS_USED) as TOTAL_DOLLARS_USED
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
        where WMH.START_TIME between '{start_date}' and '{end_date}' group by 1 order by 1 asc
        """
        return self.query_to_df(sql)

    def cost_of_materialized_views_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        select
            database_name
            ,schema_name
            ,table_name
            ,credits_used
            ,({credit_val}*credits_used) as total_dollars_used
            ,start_time
            ,end_time
        from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 6 desc;
        """
        return self.query_to_df(sql)

    def cost_of_materialized_views(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        select
            database_name
            ,schema_name
            ,table_name
            ,sum(credits_used) as credits_used
            ,({credit_val}*credits_used) as total_dollars_used
        from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3,5
        order by 1 asc;
        """
        return self.query_to_df(sql)

    def cost_of_replication_ts(self, start_date='2022-01-01', end_date=''):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        select
            database_name
            ,credits_used
            ,({credit_val}*credits_used) as total_dollars_used
            ,start_time
            ,end_time
        from {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 4 desc;
        """

        return self.query_to_df(sql)

    def cost_of_replication(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        select
            database_name
            ,sum(credits_used) as credits_used
            ,({credit_val}*credits_used) as total_dollars_used
        from {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1, 3
        order by 1 asc;
        """

        return self.query_to_df(sql)

    def cost_of_searchoptimization_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
         select
             database_name
             ,schema_name
             ,table_name
             ,sum(credits_used) as credits_used
             ,({credit_val}*credits_used) as total_dollars_used
         from {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
         where start_time between '{start_date}' and '{end_date}'
         group by 1,2,3,5
         order by 1 desc;
        """

        return self.query_to_df(sql)

    def cost_of_searchoptimization(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
         select
             database_name
             ,schema_name
             ,table_name
             ,sum(credits_used) as credits_used
             ,({credit_val}*credits_used) as total_dollars_used
         from {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
         where start_time between '{start_date}' and '{end_date}'
         group by 1,2,3,5
         order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_snowpipe_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
          select
            pipe_name
            ,credits_used
            ,start_time
            ,end_time
            ,({credit_val}*credits_used) as total_dollars_used
          from {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
          where start_time between '{start_date}' and '{end_date}'
          order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_snowpipe(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
          select
            pipe_name
            ,sum(credits_used) as credits_used
            ,({credit_val}*credits_used) as total_dollars_used
          from {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
          where start_time between '{start_date}' and '{end_date}'
          group by 1, 3
          order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_storage_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO Update docstring
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql = f"""
         select cost.WAREHOUSE_NAME, cost.USAGE_DATE, cost.DOLLARS_USED,
        sum(SUM(DOLLARS_USED)) OVER (order by cost.USAGE_DATE ASC) as Cumulative_Credits_Total from (
        SELECT
                'Storage' AS WAREHOUSE_NAME
                ,SU.USAGE_DATE
                ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
        from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
        JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH
        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM
        GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA
        ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')) as cost
        where cost.usage_date between '{start_date}' and '{end_date}' group by 1, 2, 3 order by 2 asc;
        """
        return self.query_to_df(sql)
