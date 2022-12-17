from datetime import date,datetime,time
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

    def __init__(self, connection, dbname, cache, credit_value="standard"):
        self.connection = connection
        self.dbname = dbname
        self.credit_value = credit_value
        self.cache = cache

    def simple_cache(func):
        """Wraps each of the Snowflake query and returns results from cache if files exists locally.
        Else it runs the query and saves the results to a local file.

        Args:
            func (_type_): query function

        Returns:
            Dataframe: Pandas dataframe that contains the results of the query
        """
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            cache_file = f'{self.cache}/{func.__name__}.pq'
            if os.path.exists(cache_file):
                print('Loading data from cache...')
                df = pd.read_parquet(pathlib.Path(cache_file))
            else:
                print('Loading data from Snowflake...')
                df =  func(self, *args, **kwargs)

            df.to_parquet(cache_file)
            return df

        return wrapper

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

    def ts_remove_localization(self, df):
        df["hourly_start_time"] = [d.tz_localize(None) for d in df["hourly_start_time"]]
        #df["end_time"] = [d.tz_localize(None) for d in df["end_time"]]
        return df

    ##@simple_cache
    def total_cost_breakdown_ts(self, start_date='2022-01-01', end_date=''):
        """
        Calculates the total credits consumed in a selected time period grouped by
        services consuming the credits along with cost of credits consumed calculated
        according to selected account type.
        Outputs a dataframe with the following columns and rows:
        Cost Category: Name of the service consuming the credit
        Total Credits: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type.
        Following are the rows returned:
        Compute: Total cost of compute used during the billing period
        Storage: Total cost of storage used during the billing period
        Cloud Services: Total cost of cloud services used during the billing period
        Autoclustering: Total cost of autoclustering events during the billing period
        Materialized view: Total cost consumed by materialized view during the billing period
        Search Optimization: Total cost of search optimization used during the billing period
        Snowpipe: Total cost of snowpipe usage during the billing period
        Replication: Total cost of replication done during the billing period
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        storage_df = self.cost_of_storage_ts(start_date, end_date)
        compute_df = self.cost_of_compute_ts(start_date, end_date)
        cloud_service_df = self.cost_of_cloud_services_ts(start_date, end_date)
        material_df = self.cost_of_materialized_views_ts(start_date, end_date)

        replication_df = self.cost_of_replication_ts(start_date, end_date)
        searchopt_df = self.cost_of_searchoptimization_ts(start_date, end_date)
        snowpipe_df = self.cost_of_snowpipe_ts(start_date, end_date)
        autocluster_df = self.cost_of_autoclustering_ts(start_date, end_date)

        df_concat=pd.concat([compute_df,storage_df,cloud_service_df,material_df,replication_df,searchopt_df,snowpipe_df,autocluster_df],0)
        df_select=df_concat[['user_name','credits','dollars','hourly_start_time','category_name']]

        return df_select

    # @simple_cache
    ##TODO: This can be consolidated with cost_by_wh except this one right now
    ## seems to not be taking cloud service cost into account while cost_by_wh is
    def cost_by_wh_ts(self, start_date='2022-01-01', end_date=''):
        """
        Returns results only for ACCOUNTADMIN role or any other role that has been granted MONITOR USAGE global privilege
        Calculates the total cost of compute and cloud services in a time
        series according to warehouse for a given time period using Warehouse
        Metering History table.
        Outputs a dataframe with the following columns:
        Warehouse Name: Name of the Warehouse
        Credits used: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        Start time: The start time of the billing period
        End time: The end time of the billing period
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
                select warehouse_name
                      ,credits_used as credits
                      ,({credit_val}*credits) as dollars
                      ,credits_used_cloud_services as cloud_services_credits
                      ,({credit_val}*credits_used_cloud_services) as cloud_services_dollars
                      ,date_trunc('hour', start_time) as hourly_start_time
                from {self.dbname}.account_usage.warehouse_metering_history
                where start_time between '{start_date}' and '{end_date}' -->= dateadd(day, -5, current_timestamp())
                order by 4 asc;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_autoclustering_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the cost of autoclustering in time series for a given time period using Automatic
        Clustering History table. Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Database name: The database name of the table that was clustered
        Schema name: The schema name of the table that was clustered
        Table name: The name of the table that was clustered
        Total credits used: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
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
              ,sum(credits_used) as credits
              ,({credit_val}*credits) as dollars
              ,date_trunc('hour', start_time) as hourly_start_time
              ,'Autoclustering' as category_name

        from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3,6
        order by 6 desc;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_cloud_services_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the cost of cloud services in time series for a given time period using Snowflake Warehouse
        Metering History tables. Outputs a dataframe with the following columns:
        WAREHOUSE_NAME: Name of the warehouse
        CREDITS_USED: Cloud Services Credits used during a selected billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        START_TIME: Start date and time of the billing period
        END_TIME: End date and time of the billing period
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        SELECT DISTINCT
                'Snowflake' as user_name
                ,WMH.WAREHOUSE_NAME
                ,WMH.CREDITS_USED_CLOUD_SERVICES as credits
                ,({credit_val}*credits) as dollars
                ,date_trunc('hour',WMH.start_time) as hourly_start_time
                ,'Cloud services' as category_name

        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;
        """

        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_compute_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the cost of compute in time series for a given time period using Snowflake Warehouse
        Metering History tables. Outputs a dataframe with the following columns:
        Warehouse Name: Name of the warehouse
        credits used: Compute credits used during the billing period
        Start time: The start time of the billing period
        End time: The end time of the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
        SELECT DISTINCT
                 WEH.USER_NAME,
                 WMH.WAREHOUSE_NAME
                ,WMH.CREDITS_USED_COMPUTE as credits
                ,({credit_val}*credits) as dollars
                ,date_trunc('hour', WMH.start_time) as hourly_start_time
                ,'Compute' as category_name
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
        where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_materialized_views_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of materialized views in time series for a given time
        period using Materialized View Refresh History table.
        Outputs a dataframe with the following columns:
        Start time: The start time of the billing period
        End time: The end time of the billing period
        Database name: The database name of the table
        Schema name: The schema name of the table that was clustered
        Table name: The name of the table that was clustered
        Total credits used: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        """
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
            ,credits_used as credits
            ,'Snowflake' as user_name
            ,({credit_val}*credits) as dollars
            ,date_trunc('hour', start_time) as hourly_start_time
            ,'Materialized views' as category_name
        from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 6 desc;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_replication_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the cost of replication in time series used in a given time
        period using Replication Usage History table.
        Outputs a dataframe with the following columns:
        Database name: The database name of the table
        Total credits used: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]

        sql=f"""
        select
            'Snowflake' as user_name,
             database_name
            ,credits_used as credits
            ,({credit_val}*credits) as dollars
            ,date_trunc('hour', start_time) as hourly_start_time
            ,'Replication' as category_name
        from {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 4 desc;
        """

        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_searchoptimization_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates thCREDITS_USEDe cost of search optimizations in time series used in a
        given time period using Search Optimization History table.
        Outputs a dataframe with the following columns:
        Database name: The database name of the table on which search optimization is applied
        Schema name: The schema name on which search optimization is applied
        Total credits used: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        """
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
            ,credits_used as credits
            ,({credit_val}*credits) as dollars
            ,date_trunc('hour', start_time) as hourly_start_time
            ,'Snowflake' as user_name
            ,'Search optimization' as category_name
        from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 6 desc;"""

        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_snowpipe_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the cost of snowpipe usage in time series in a
        given time period using Pipe Usage History table.
        Outputs a dataframe with the following columns:
        Pipe name: Name of the snowpipe used
        Start time: The start date and time of the billing period
        End time: The end date and time of the billing period
        Credits used: Total credits used during the billing period
        TOTAL_DOLLARS_USED: Total cost of credits (in dollars) used during a
        selected billing period calculated according to selected account type
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        credit_val = ''
        if self.credit_value:
            credit_val = SNFLKQuery.credit_values[self.credit_value]
        sql = f"""
          select
            'Snowflake' as user_name
            ,pipe_name
            ,credits_used as credits
            ,date_trunc('hour', start_time) as hourly_start_time
            ,({credit_val}*credits) as dollars
            ,'Snowpipe' as category_name
          from {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
          where start_time between '{start_date}' and '{end_date}'
          order by 1 desc;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_of_storage_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO: Distribute daily storage costs hourly over the day so that ts is consistent with other ts
        """
        Calculates the overall cost of storage usage
        given time period using Storage Usage Su table.
        Outputs a dataframe with the fhollowing columns:
        Category name: Category name as Storage
        Usage date: The date on which storage is used
        Dollars used: Total cost of storage (in dollars) used
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql = f"""
        select cost.category_name, cost.USAGE_DATE as start_time, cost.DOLLARS_USED as dollars, 'Snowflake' as user_name, 0 as credits from (
        SELECT

                'Storage' as category_name
                ,SU.USAGE_DATE


                ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH as DOLLARS_USED
        from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
        JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH
        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM
        GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA
        ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')) as cost
        where cost.usage_date between '{start_date}' and '{end_date}' group by 1, 2, 3 order by 2 asc;
        """
        df = self.query_to_df(sql)
        # Returns an unlocalized time
        df = df.set_index('start_time').resample('1H').ffill()
        df['dollars'] = df['dollars']/24.
        df.reset_index(inplace=True)
        df.insert(1, "end_time", df["start_time"] + pd.offsets.Hour(1))
        return df

    def cost_by_partner_tool_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO: Convert partner tool consumption query into
        # a hourly timeseries so that its consistent with others and can be plotted out
        """
        Calculates the overall cost of usage by partner application over a
        Outputs a dataframe with the following columns:
        Category name: Category name as Storage
        Usage date: The date on which storage is used
        Dollars used: Total cost of storage (in dollars) used
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        
        sql=f"""
            --THIS IS APPROXIMATE CREDIT CONSUMPTION BY CLIENT APPLICATION
            WITH CLIENT_HOUR_EXECUTION_CTE AS (
                SELECT  CASE
                    WHEN CLIENT_APPLICATION_ID LIKE 'Go %' THEN 'Go'
                    WHEN CLIENT_APPLICATION_ID LIKE 'Snowflake UI %' THEN 'Snowflake UI'
                    WHEN CLIENT_APPLICATION_ID LIKE 'SnowSQL %' THEN 'SnowSQL'
                    WHEN CLIENT_APPLICATION_ID LIKE 'JDBC %' THEN 'JDBC'
                    WHEN CLIENT_APPLICATION_ID LIKE 'PythonConnector %' THEN 'Python'
                    WHEN CLIENT_APPLICATION_ID LIKE 'ODBC %' THEN 'ODBC'
                    ELSE 'NOT YET MAPPED: ' || CLIENT_APPLICATION_ID
                END AS CLIENT_APPLICATION_NAME
                ,WAREHOUSE_NAME
                ,DATE_TRUNC('hour',START_TIME) as hourly_start_time
                ,SUM(EXECUTION_TIME)  as CLIENT_HOUR_EXECUTION_TIME
                FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
                JOIN {self.dbname}.ACCOUNT_USAGE.SESSIONS SE ON SE.SESSION_ID = QH.SESSION_ID
                WHERE WAREHOUSE_NAME IS NOT NULL
                AND EXECUTION_TIME > 0
            
            --Change the below filter if you want to look at a longer range than the last 1 month 
                AND START_TIME  between '{start_date}' and '{end_date}'
                group by 1,2,3
                )
            , HOUR_EXECUTION_CTE AS (
                SELECT  hourly_start_time
                ,WAREHOUSE_NAME
                ,SUM(CLIENT_HOUR_EXECUTION_TIME) AS HOUR_EXECUTION_TIME
                FROM CLIENT_HOUR_EXECUTION_CTE
                group by 1,2
            )
            , APPROXIMATE_CREDITS AS (
                SELECT 
                A.CLIENT_APPLICATION_NAME
                ,C.WAREHOUSE_NAME
                ,(A.CLIENT_HOUR_EXECUTION_TIME/B.HOUR_EXECUTION_TIME)*C.CREDITS_USED AS APPROXIMATE_CREDITS_USED
                ,A.hourly_start_time as hourly_start_time
                FROM CLIENT_HOUR_EXECUTION_CTE A
                JOIN HOUR_EXECUTION_CTE B  ON A.hourly_start_time = B.hourly_start_time and B.WAREHOUSE_NAME = A.WAREHOUSE_NAME
                JOIN {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY C ON C.WAREHOUSE_NAME = A.WAREHOUSE_NAME AND C.START_TIME = A.hourly_start_time
            )

            SELECT 
            CLIENT_APPLICATION_NAME
            ,WAREHOUSE_NAME
            ,SUM(APPROXIMATE_CREDITS_USED) AS APPROXIMATE_CREDITS_USED
            ,hourly_start_time
            FROM APPROXIMATE_CREDITS
            GROUP BY 1,2,4
            ORDER BY 3 DESC
            ;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        # df = self.ts_remove_localization(self.query_to_df(sql))
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    ## Config related queries
    def warehouse_config(self):
        """Gives the details of the wareouse config"""
        sql = f"""select * from {self.dbname}.account_usage.warehouses"""
        df = self.query_to_df(sql)
        return df


    ## Query cost related queries
    ##TODO: 1) Check query 2) Add flag to give unique query text with parameters
    def n_expensive_queries(self, start_date='2022-01-01', end_date='', n=10):
        """
        Calculates expense of queries over a specific time period
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
                ,QH.EXECUTION_TIME
                ,QH.WAREHOUSE_SIZE
            FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
            WHERE START_TIME between '{start_date}' and '{end_date}'
        )

        SELECT QH.QUERY_ID
            
            ,QH.QUERY_TEXT
            ,QH.USER_NAME
            ,QH.ROLE_NAME
            ,(QH.EXECUTION_TIME/(1000*60*60)) AS EXECUTION_TIME_HOURS
            ,WS.WAREHOUSE_SIZE
            ,WS.NODES
            ,QH.WAREHOUSE_NAME
            ,QH.BYTES_SPILLED_TO_REMOTE_STORAGE
            
            ,(QH.EXECUTION_TIME/(1000*60*60))*WS.NODES as RELATIVE_PERFORMANCE_COST
            ,QH.PARTITIONS_SCANNED
            ,QH.PARTITIONS_TOTAL
            ,QH.CLUSTER_NUMBER
            ,QH.QUERY_TYPE
            ,QH.EXECUTION_STATUS
            ,QH.DATABASE_NAME
            ,QH.SCHEMA_NAME
            ,QH.TOTAL_ELAPSED_TIME

        FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        ORDER BY RELATIVE_PERFORMANCE_COST DESC
        LIMIT {n}
        ;
        """
        df = self.query_to_df(sql)
        return df

    def n_queries_spill_to_storage(self, start_date='2022-01-01', end_date='', n=10):
        """
        Shows queries spilling maximum remote storage
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql = f"""
        select query_text, user_name, role_name, warehouse_name, warehouse_size,
        BYTES_SPILLED_TO_REMOTE_STORAGE, total_elapsed_time/1000 total_elapsed_time_seconds
        from   {self.dbname}.account_usage.query_history
        where  BYTES_SPILLED_TO_REMOTE_STORAGE > 0
        and TO_DATE(start_time) between '{start_date}' and '{end_date}'
        order  by BYTES_SPILLED_TO_REMOTE_STORAGE desc
        limit {n};
        """
        df = self.query_to_df(sql)
        return df
    
    def n_queries_scanned_most_data(self, start_date='2022-01-01',end_date='2022-02-02',n=10):
        """
        Shows queries that scan the most data
        """
        
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql= f"""
        select
          
          QUERY_ID
         ,QUERY_TEXT
         ,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
         ,PARTITIONS_SCANNED
         ,PARTITIONS_TOTAL

        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY 
        where 1=1
        and TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
            and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
            and ERROR_CODE iS NULL
            and PARTITIONS_SCANNED is not null
        
        
        
        LIMIT {n}
        """
        df=self.query_to_df(sql)
        return df
    
    def n_most_cached_queries(self, start_date='2022-01-01',end_date=''):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f""" 
        SELECT 
        QUERY_ID
        
        
        ,BYTES_SCANNED
        ,BYTES_SCANNED*PERCENTAGE_SCANNED_FROM_CACHE AS BYTES_SCANNED_FROM_CACHE
        ,BYTES_SCANNED*PERCENTAGE_SCANNED_FROM_CACHE /BYTES_SCANNED AS PERCENT_SCANNED_FROM_CACHE
        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
        AND BYTES_SCANNED > 0
        
        ORDER BY 4 
        """
       
        
        df=self.query_to_df(sql)
        return df
    
    def n_most_executed_queries(self, start_date='2022-01-01',end_date=''):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT 
        QUERY_TEXT
        ,count(*) as number_of_queries
        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY 
        where 1=1
        and TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
        and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
        group by 1
        having count(*) >= 10 --configurable/minimal threshold
        order by 2 desc
        limit 100 --configurable upper bound threshold
        """

        df=self.query_to_df(sql)
        return df
        
    #Do we want 30 day logins
    def idle_users(self, start_date='2022-01-01',end_date=''):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT *
        FROM {self.dbname}.ACCOUNT_USAGE.USERS 
        WHERE LAST_SUCCESS_LOGIN < DATEADD(month, -1, CURRENT_TIMESTAMP()) 
        AND DELETED_ON IS NULL
        """
        df=self.query_to_df(sql)
        return df
        
    def users_full_table_scans(self, start_date='2022-01-01',end_date='',n=10):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT USER_NAME
        ,COUNT(*) as COUNT_OF_QUERIES
        FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= dateadd(month,-1,current_timestamp())
        AND PARTITIONS_SCANNED > (PARTITIONS_TOTAL*0.95)
        AND QUERY_TYPE NOT LIKE 'CREATE%'
        group by 1
        order by 2 desc;
        
        """
        df=self.query_to_df(sql)
       
        return df
    def heavy_users(self, start_date='2022-01-01',end_date='',n=10):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        select 
        User_name
        , warehouse_name
        , avg(case when partitions_total > 0 then partitions_scanned / partitions_total else 0 end) avg_pct_scanned
        from   {self.dbname}.account_usage.query_history
        where  start_time::date > dateadd('days', -45, current_date)
        group by 1, 2
        order by 3 desc"""
        df=self.query_to_df(sql)
        return df
    def users_never_logged_in(self,start_date="2022-02-02", end_date=""):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT *
        FROM {self.dbname}.ACCOUNT_USAGE.USERS 
        WHERE LAST_SUCCESS_LOGIN IS NULL;
        """
        df=self.query_to_df(sql)
        return df
    def idle_roles(self,start_date="2022-01-01", end_date=""):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)

        sql=f"""
        SELECT 
        R.*
        FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES R
        LEFT JOIN (
            SELECT DISTINCT 
                ROLE_NAME 
            FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE START_TIME > DATEADD(month,-1,CURRENT_TIMESTAMP())
                ) Q 
                        ON Q.ROLE_NAME = R.NAME
        WHERE Q.ROLE_NAME IS NULL
        and DELETED_ON IS NULL;
        """
        df=self.query_to_df(sql)
        return df
    def failed_tasks(self,start_date="2022-01-01", end_date=""):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        select *
        from {self.dbname}.account_usage.task_history
        WHERE STATE = 'FAILED'
        and query_start_time >= DATEADD (day,-7, CURRENT_TIMESTAMP())
        order by query_start_time DESC
        
        """
        df=self.query_to_df(sql)
        return df
    def long_running_tasks(self,start_date="2022-01-01", end_date=""):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        select DATEDIFF(seconds, QUERY_START_TIME,COMPLETED_TIME) as DURATION_SECONDS
        ,*
        from {self.dbname}.account_usage.task_history
        WHERE STATE = 'SUCCEEDED'
        and query_start_time >= DATEADD (day, -7, CURRENT_TIMESTAMP())
        order by DURATION_SECONDS desc
        """
        df=self.query_to_df(sql)
        return df
    # They will show up if they have been accessed
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



        
        
        
    


        
   
    

  
