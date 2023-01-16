from .snflk import SNFLKQuery
import pandas as pd
# Initialize analysis dates
from datetime import datetime
from dateutil.relativedelta import relativedelta

## Function for date time analysis
##TODO: Move to a library function
def get_previous_dates(sdate, edate, date_shift_months):
    sdate_datetime = datetime.strptime(sdate,'%Y-%m-%d')
    prev_sdates_datetime = datetime.strptime(sdate,'%Y-%m-%d') - relativedelta(months=date_shift_months)
    prev_sdates = prev_sdates_datetime.strftime("%Y-%m-%d")
    edate_datetime = datetime.strptime(edate,'%Y-%m-%d')
    prev_edates_datetime = datetime.strptime(edate,'%Y-%m-%d') - relativedelta(months=date_shift_months)
    prev_edates = prev_edates_datetime.strftime("%Y-%m-%d")
    return prev_sdates, prev_edates

class CostProfile(SNFLKQuery):

    """Cost related query and support functions"""
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
        # df_select=df_concat[['user_name','credits','dollars','hourly_start_time','category_name']]

        return df_concat

    # @simple_cache
    ##TODO: This can be consolidated with cost_by_wh except this one right now
    ## seems to not be taking cloud service cost into account while cost_by_wh is
    def cost_by_wh_ts(self, start_date='2022-01-01', end_date=''):
        """
        Returns results only for ACCOUNTADMIN role or any other role that has been granted MONITOR USAGE global privilege
        https://docs.snowflake.com/en/sql-reference/functions/warehouse_metering_history.html
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
                where start_time between '{start_date}' and '{end_date}'
                order by 4 asc;
        """

        df = self.query_to_df(sql)
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
        
        df = self.query_to_df(sql)
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

        df = self.query_to_df(sql)
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
        select
            warehouse_name
            ,credits_used_compute as credits
            ,({credit_val}*credits_used_compute) as dollars
            ,date_trunc('hour', WMH.start_time) as hourly_start_time
            ,'Compute' as category_name
         from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
         where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;      
        """
        
        df = self.query_to_df(sql)
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
        
        df = self.query_to_df(sql)
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

        df = self.query_to_df(sql)
        return df

    # @simple_cache
    def cost_of_searchoptimization_ts(self, start_date='2022-01-01', end_date=''):
        """Calculates the cost of search optimizations in time series used in a
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

        df = self.query_to_df(sql)
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
        
        df = self.query_to_df(sql)
        return df

    # @simple_cache
    def cost_of_storage_ts(self, start_date='2022-01-01', end_date=''):
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
        select 
            cost.category_name
            ,cost.USAGE_DATE::timestamp_ltz as start_time
            ,cost.DOLLARS_USED as dollars
            ,'Snowflake' as user_name
            ,0 as credits from (
            select
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
        df.rename(columns = {'start_time':'hourly_start_time'}, inplace = True)
        return df

    def cost_by_user_ts(self, start_date='2022-01-01', end_date=''):
        ##TODO: @Manasvini to review
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
            --THIS IS APPROXIMATE CREDIT CONSUMPTION BY USER
            WITH USER_HOUR_EXECUTION_CTE AS (
                SELECT  USER_NAME
                ,WAREHOUSE_NAME
                ,DATE_TRUNC('hour',START_TIME) as hourly_start_time
                ,SUM(EXECUTION_TIME)  as USER_HOUR_EXECUTION_TIME
                FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY QH
                WHERE WAREHOUSE_NAME IS NOT NULL
                AND EXECUTION_TIME > 0
            
            --Change the below filter if you want to look at a longer range than the last 1 month 
                AND START_TIME  between '{start_date}' and '{end_date}'
                group by 1,2,3
                )
            , HOUR_EXECUTION_CTE AS (
                SELECT  hourly_start_time
                ,WAREHOUSE_NAME
                ,SUM(USER_HOUR_EXECUTION_TIME) AS HOUR_EXECUTION_TIME
                FROM USER_HOUR_EXECUTION_CTE
                group by 1,2
            )
            , APPROXIMATE_CREDITS AS (
                SELECT 
                A.USER_NAME
                ,C.WAREHOUSE_NAME
                ,(A.USER_HOUR_EXECUTION_TIME/B.HOUR_EXECUTION_TIME)*C.CREDITS_USED AS APPROXIMATE_CREDITS_USED
                ,A.hourly_start_time as hourly_start_time
                FROM USER_HOUR_EXECUTION_CTE A
                JOIN HOUR_EXECUTION_CTE B  ON A.hourly_start_time = B.hourly_start_time and B.WAREHOUSE_NAME = A.WAREHOUSE_NAME
                JOIN {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY C ON C.WAREHOUSE_NAME = A.WAREHOUSE_NAME AND C.START_TIME = A.hourly_start_time
            )

            SELECT 
            USER_NAME
            ,WAREHOUSE_NAME
            ,SUM(APPROXIMATE_CREDITS_USED) AS APPROXIMATE_CREDITS_USED
            ,hourly_start_time
            FROM APPROXIMATE_CREDITS
            GROUP BY 1,2,4
            ORDER BY 3 DESC
            ;
        """
        df = self.query_to_df(sql)
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
                    WHEN CLIENT_APPLICATION_ID LIKE '.NET %' THEN '.NET'
                    WHEN CLIENT_APPLICATION_ID LIKE 'JavaScript %' THEN 'Javascript'
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

        df = self.query_to_df(sql)
        return df