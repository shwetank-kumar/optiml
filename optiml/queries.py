from datetime import date,datetime,time
import functools
import os
import pandas as pd
import pathlib

class SNFLKQuery():
    credit_values = {
    "standard": 2,
    "enterprise": 3,
    "business critical": 4
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
        df["start_time"] = [d.tz_localize(None) for d in df["start_time"]]
        df["end_time"] = [d.tz_localize(None) for d in df["end_time"]]
        return df

    ##TODO: Add a decorator to read from a materialized view if available
    # @simple_cache
    def total_cost_breakdown(self, start_date='2022-01-01', end_date=''):
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
        usage_list = []
        storage_df = self.cost_of_storage_ts(start_date, end_date)
        compute_df = self.cost_of_compute_ts(start_date, end_date)
        cloud_service_df = self.cost_of_cloud_services_ts(start_date, end_date)
        material_df = self.cost_of_materialized_views_ts(start_date, end_date)
        replication_df = self.cost_of_replication_ts(start_date, end_date)
        searchopt_df = self.cost_of_searchoptimization_ts(start_date, end_date)
        snowpipe_df = self.cost_of_snowpipe_ts(start_date, end_date)
        autocluster_df = self.cost_of_autoclustering_ts(start_date, end_date)
        storage_sum = storage_df["dollars"].sum()
        storage_credits = 0
        usage_list.append(["Storage", storage_credits, storage_sum])
        compute_sum = compute_df["dollars"].sum()
        compute_credits = compute_df["credits"].sum()
        usage_list.append(["Compute", compute_credits, compute_sum])
        cloud_service_sum = cloud_service_df["dollars"].sum()
        cloud_services_credits = cloud_service_df["credits"].sum()
        usage_list.append(["Cloud Service", cloud_services_credits, cloud_service_sum])
        autocluster_sum = autocluster_df["dollars"].sum()
        autocluster_credits = autocluster_df["credits"].sum()
        usage_list.append(["Autoclustering", autocluster_credits, autocluster_sum])
        material_sum = material_df["dollars"].sum()
        material_credits = material_df["credits"].sum()
        usage_list.append(["Materialization Views", material_credits, material_sum])
        replication_sum = replication_df["dollars"].sum()
        replication_credits = replication_df["dollars"].sum()
        usage_list.append(["Replication", replication_credits, replication_sum])
        searchopt_sum = searchopt_df["dollars"].sum()
        searchopt_credits = searchopt_df["credits"].sum()
        usage_list.append(["Search Optimization", searchopt_credits, searchopt_sum])
        snowpipe_sum = snowpipe_df["dollars"].sum()
        snowpipe_credits = snowpipe_df["credits"].sum()
        usage_list.append(["Snowpipe", snowpipe_credits, snowpipe_sum])
        sqldf = pd.DataFrame(data = usage_list, columns=["cost_category", "credits", "dollars"])
        return sqldf


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
        df_select=df_concat[['user_name','credits','dollars','start_time','end_time','category_name']]

        return df_select


    #TODO:
    # @simple_cache
    def cost_by_user(self, start_date, end_date):
        pass

    # @simple_cache


    # @simple_cache
    ##TODO: This can be consolidated with cost_by_wh except this one right now
    ## seems to not be taking cloud service cost into account while cost_by_wh is
    def cost_by_wh_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the total cost of compute and cloud services in a time
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
                      ,start_time
                      ,end_time
                from {self.dbname}.account_usage.warehouse_metering_history
                where start_time between '{start_date}' and '{end_date}' -->= dateadd(day, -5, current_timestamp())
                group by 1,2,4,5
                order by 4 asc;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
        df = self.ts_remove_localization(self.query_to_df(sql))
        return df

    # @simple_cache
    def cost_by_wh(self, start_date='2022-01-01', end_date=''):
        """Calculates the total cost of compute and cloud services according to
        warehouse for a given time period using Warehouse Metering History table.
        Outputs a dataframe with the following columns:
        Warehouse Name: Name of the Warehouse
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
                select warehouse_name
                      ,sum(credits_used) as credits
                      ,({credit_val}*credits) as dollars
                from {self.dbname}.account_usage.warehouse_metering_history
                where start_time between '{start_date}' and '{end_date}' -->= dateadd(day, -5, current_timestamp())
                group by 1
                order by 1 desc;
        """
        return self.query_to_df(sql)

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
              ,start_time
              ,end_time
              ,'Autoclustering' as category_name

        from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3,6,7
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
                ,WMH.START_TIME
                ,WMH.END_TIME
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
                ,WMH.START_TIME
                ,WMH.END_TIME
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
            ,start_time
            ,end_time
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
            ,start_time
            ,end_time
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
            ,start_time
            ,end_time
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
            ,start_time
            ,end_time
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
        # sql = f"""-- THIS IS APPROXIMATE CREDIT CONSUMPTION BY CLIENT APPLICATION
        #     with client_hour_execution_cte as (
        #         select  case
        #             when client_application_id like 'Go %' then 'Go'
        #             when client_application_id like 'Snowflake UI %' then 'Snowflake UI'
        #             when client_application_id like 'SnowSQL %' then 'SnowSQL'
        #             when client_application_id like 'JDBC %' then 'JDBC'
        #             when client_application_id like 'PythonConnector %' then 'Python'
        #             when client_application_id like 'ODBC %' then 'ODBC'
        #             else 'NOT YET MAPPED: ' || client_application_id
        #             end as client_application_name
        #         ,warehouse_name
        #         ,date_trunc('hour',start_time) as start_time_hour
        #         ,sum(execution_time)  as client_hour_execution_time
        #         from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY qh
        #         join {self.dbname}.ACCOUNT_USAGE.SESSIONS se on se.session_id = qh.session_id
        #         where warehouse_name is not null
        #             and execution_time > 0

        #     -- Change the below filter if you want to look at a longer range than the last 1 month
        #             and start_time between '{start_date}' and '{end_date}'
        #         group by 1,2,3
        #         )
        #     , hour_execution_cte as (
        #         select start_time_hour
        #             ,warehouse_name
        #             ,sum(client_hour_execution_time) as hour_execution_time
        #         from client_hour_execution_cte
        #         group by 1,2
        #     )
        #     , approximate_credits as (
        #         select
        #             a.client_application_name
        #             ,c.warehouse_name
        #             ,(a.client_hour_execution_time/b.hour_execution_time)*c.credits_used as approximate_credits_used

        #         from client_hour_execution_cte a
        #         join hour_execution_cte b  on a.start_time_hour = b.start_time_hour and b.warehouse_name = a.warehouse_name
        #         join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY c on c.warehouse_name = a.warehouse_name and c.start_time = a.start_time_hour
        #     )

        #     select
        #         client_application_name
        #         ,warehouse_name
        #         ,sum(approximate_credits_used) as approximate_credits_used
        #     from approximate_credits
        #     group by 1,2
        #     order by 3 desc
        #     ;"""
        sql = f"""-- THIS IS A TIMESERIES OF APPROXIMATE CREDIT CONSUMPTION BY CLIENT APPLICATION
            with client_hour_execution_cte as (
                select  case
                    when client_application_id like 'Go %' then 'Go'
                    when client_application_id like 'Snowflake UI %' then 'Snowflake UI'
                    when client_application_id like 'SnowSQL %' then 'SnowSQL'
                    when client_application_id like 'JDBC %' then 'JDBC'
                    when client_application_id like 'PythonConnector %' then 'Python'
                    when client_application_id like 'ODBC %' then 'ODBC'
                    else 'NOT YET MAPPED: ' || client_application_id
                    end as client_application_name
                ,warehouse_name
                ,date_trunc('hour',start_time) as start_time_hour
                ,sum(execution_time)  as client_hour_execution_time
                from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY qh
                join {self.dbname}.ACCOUNT_USAGE.SESSIONS se on se.session_id = qh.session_id
                where warehouse_name is not null
                    and execution_time > 0
            -- Change the below filter if you want to look at a longer range than the last 1 month
                    and start_time between '{start_date}' and '{end_date}'
                group by 1,2,3
                )
            , hour_execution_cte as (
                select start_time_hour
                    ,warehouse_name
                    ,sum(client_hour_execution_time) as hour_execution_time
                from client_hour_execution_cte
                group by 1,2
            )
            , approximate_credits as (
                select
                    date_trunc('day', a.start_time_hour) as truncated_date
                    ,a.client_application_name
                    ,c.warehouse_name
                    ,(a.client_hour_execution_time/b.hour_execution_time)*c.credits_used as approximate_credits_used
                from client_hour_execution_cte a
                join hour_execution_cte b  on a.start_time_hour = b.start_time_hour and b.warehouse_name = a.warehouse_name
                join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY c on c.warehouse_name = a.warehouse_name and c.start_time = a.start_time_hour
            )
            select
                truncated_date
                ,client_application_name
                ,sum(approximate_credits_used) as approximate_credits_used
            from approximate_credits
            group by 1,2
            order by 1 asc, 3 desc
            ;
        """
        ## Removing localization on the timestamp so it can bite us in the ass
        ## later
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
              ,(QH.EXECUTION_TIME/(1000*60)) AS EXECUTION_TIME_MINUTES
              ,WS.WAREHOUSE_SIZE
              ,WS.NODES
              ,WC.CREDITS
              ,(WC.DOLLARS/QC.QUERY_COUNT) as cost_per_query
        FROM QUERY_HISTORY QH
        JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
        JOIN  (
            SELECT
               WAREHOUSE_NAME
              ,COUNT(QUERY_ID) as QUERY_COUNT
            FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
            GROUP BY WAREHOUSE_NAME
              ) QC
        JOIN (
            SELECT
                WAREHOUSE_NAME
                ,SUM(CREDITS_USED) as CREDITS
                ,SUM(CREDITS_USED)*({credit_val}) as DOLLARS
            FROM {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE TO_DATE(START_TIME) between '{start_date}' and '{end_date}'
            GROUP BY WAREHOUSE_NAME
          ) WC
        ORDER BY CREDITS DESC LIMIT {n}
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
    
    def n_scanned_data(self, start_date='2022-01-01',end_date='2022-02-02',n=10):
        """
        Shows queries that scan the most data
        """
        
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql= f"""
        select
          
          QUERY_ID
          
         
         ,ROW_NUMBER() OVER(ORDER BY PARTITIONS_SCANNED DESC) as QUERY_ID_INT
         ,QUERY_TEXT
         ,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
         ,PARTITIONS_SCANNED
         ,PARTITIONS_TOTAL

        from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
        where 1=1
        and TO_DATE(Q.START_TIME) >     DATEADD(month,-1,TO_DATE(CURRENT_TIMESTAMP())) 
            and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
            and ERROR_CODE iS NULL
            and PARTITIONS_SCANNED is not null
        
        order by  PARTITIONS_SCANNED desc
        
        LIMIT {n}
        """
        df=self.query_to_df(sql)
        return df