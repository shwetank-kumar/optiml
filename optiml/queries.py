from datetime import date
import pandas as pd


class SNFLKQuery():
    credit_values = {
    "standard": 2,
    "enterprise": 3,
    "business critical": 4
    }
    # set first value below for int
    data_type_map = ['float','float','string','datetime','datetime','string','datetime',
    'datetime','datetime','string','list','bytes','datetime','bool']
    def __init__(self, connection, dbname, credit_value="standard"):
        self.connection = connection
        self.dbname = dbname
        self.credit_value = credit_value
    
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
        return data_one

        
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
        compute_df = self.cost_of_compute(start_date, end_date)
        cloud_service_df = self.cost_of_cloud_services(start_date, end_date)
        material_df = self.cost_of_materialized_views(start_date, end_date)
        replication_df = self.cost_of_replication(start_date, end_date)
        searchopt_df = self.cost_of_searchoptimization(start_date, end_date)
        snowpipe_df = self.cost_of_snowpipe(start_date, end_date)
        autocluster_df = self.cost_of_autoclustering(start_date, end_date)

        storage_sum = float(storage_df["DOLLARS"].sum())
        storage_credits = 0
        usage_list.append(["Storage", storage_credits, storage_sum])
        compute_sum = float(compute_df["DOLLARS"].sum())
        compute_credits = float(compute_df["CREDITS"].sum())
        usage_list.append(["Compute", compute_credits, compute_sum])
        cloud_service_sum = float(cloud_service_df["DOLLARS"].sum())
        cloud_services_credits = float(cloud_service_df["CREDITS"].sum())
        usage_list.append(["Cloud Service", cloud_services_credits, cloud_service_sum])
        autocluster_sum = float(autocluster_df["DOLLARS"].sum())
        autocluster_credits = float(autocluster_df["CREDITS"].sum())
        usage_list.append(["Autoclustering", autocluster_credits, autocluster_sum])
        material_sum = float(material_df["DOLLARS"].sum())
        material_credits = float(material_df["CREDITS"].sum())
        usage_list.append(["Materialization Views", material_credits, material_sum])
        replication_sum = float(replication_df["DOLLARS"].sum())
        replication_credits = float(replication_df["CREDITS"].sum())
        usage_list.append(["Replication", replication_credits, replication_sum])
        searchopt_sum = float(searchopt_df["DOLLARS"].sum())
        searchopt_credits = float(searchopt_df["CREDITS"].sum())
        usage_list.append(["Search Optimization", searchopt_credits, searchopt_sum])
        snowpipe_sum = float(snowpipe_df["DOLLARS"].sum())
        snowpipe_credits = float(snowpipe_df["CREDITS"].sum())
        usage_list.append(["Snowpipe", snowpipe_credits, snowpipe_sum])
        sqldf = pd.DataFrame(data = usage_list, columns=["cost_category", "credits", "dollars"])
        return sqldf


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
        usage_list = []
        storage_df = self.cost_of_storage_ts(start_date, end_date)
        compute_df = self.cost_of_compute_ts(start_date, end_date)
        cloud_service_df = self.cost_of_cloud_services_ts(start_date, end_date)
        material_df = self.cost_of_materialized_views_ts(start_date, end_date)
        replication_df = self.cost_of_replication_ts(start_date, end_date)
        searchopt_df = self.cost_of_searchoptimization_ts(start_date, end_date)
        snowpipe_df = self.cost_of_snowpipe_ts(start_date, end_date)
        autocluster_df = self.cost_of_autoclustering_ts(start_date, end_date)
        ts_df = storage_df.append(compute_df)
        return ts_df

    # def cost_by_user_ts(self, start_date, end_date):
    #     ini_date = ""
    #     if start_date and end_date:
    #         ini_date = "where cost.start_time>='{}' and cost.end_time<='{}'".format(start_date, end_date)
    #     sql = f"""
    #         --COST.WAREHOUSE_GROUP_NAME, COST.USER_NAME, COST.WAREHOUSE_NAME, COST.START_TIME, COST.END_TIME, SUM(COST.CREDITS_USED), SUM(CREDIT_PRICE), SUM(DOLLARS_USED)
    #         SELECT DISTINCT cost.USER_NAME, cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_used,
    #         sum(SUM(credits_used)) OVER (order by cost.START_TIME ASC) as Cumulative_Credits_Total, cost.START_TIME, cost.END_TIME  from (
    #         SELECT DISTINCT
    #                  'WH Compute' as WAREHOUSE_GROUP_NAME,
    #                  WEH.USER_NAME
    #                 ,WMH.WAREHOUSE_NAME
    #                 ,WMH.START_TIME
    #                 ,WMH.END_TIME
    #                 ,WMH.CREDITS_USED
    #                 ,1.00 as CREDIT_PRICE
    #                 ,(1.00*WMH.CREDITS_USED) AS DOLLARS_USED
    #                 ,'ACTUAL COMPUTE' AS MEASURE_TYPE
    #         from    {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
    #         UNION
    #         --COMPUTE FROM SNOWPIPE
    #         SELECT
    #                  'Snowpipe' AS WAREHOUSE_GROUP_NAME,
    #                  'SNOWFLAKE' as user_name
    #                 ,PUH.PIPE_NAME AS WAREHOUSE_NAME
    #                 ,PUH.START_TIME
    #                 ,PUH.END_TIME
    #                 ,PUH.CREDITS_USED
    #                 ,1.00 as CREDIT_PRICE
    #                 ,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
    #                 ,'ACTUAL COMPUTE' AS MEASURE_TYPE
    #         from    {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH
    #
    #         UNION
    #
    #         --COMPUTE FROM CLUSTERING
    #         SELECT
    #                  'Auto Clustering' AS WAREHOUSE_GROUP_NAME,
    #                  'SNOWFLAKE' as user_name
    #                 ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
    #                 ,ACH.START_TIME
    #                 ,ACH.END_TIME
    #                 ,ACH.CREDITS_USED
    #                 ,1.00 as CREDIT_PRICE
    #                 ,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
    #                 ,'ACTUAL COMPUTE' AS MEASURE_TYPE
    #         from    {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH
    #
    #         UNION
    #
    #         --COMPUTE FROM MATERIALIZED VIEWS
    #         SELECT
    #                  'Materialized Views' AS WAREHOUSE_GROUP_NAME,
    #                  'SNOWFLAKE' AS user_name
    #                 ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
    #                 ,MVH.START_TIME
    #                 ,MVH.END_TIME
    #                 ,MVH.CREDITS_USED
    #                 ,1.00 as CREDIT_PRICE
    #                 ,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
    #                 ,'ACTUAL COMPUTE' AS MEASURE_TYPE
    #         from    {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
    #         UNION
    #         SELECT
    #                  'Replication' AS WAREHOUSE_GROUP_NAME,
    #                  'SNOWFLAKE' as user_name
    #                 ,DATABASE_NAME AS WAREHOUSE_NAME
    #                 ,RUH.START_TIME
    #                 ,RUH.END_TIME
    #                 ,RUH.CREDITS_USED
    #                 ,1.00 as CREDIT_PRICE
    #                 ,(1.00*RUH.CREDITS_USED) AS DOLLARS_USED
    #                 ,'ACTUAL COMPUTE' AS MEASURE_TYPE
    #         from    {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH
    #
    #         UNION
    #
    #         --STORAGE COSTS
    #         SELECT
    #                  'Storage' AS WAREHOUSE_GROUP_NAME,
    #                  'SNOWFLAKE' as user_name
    #                 ,'Storage' AS WAREHOUSE_NAME
    #                 ,SU.USAGE_DATE
    #                 ,SU.USAGE_DATE
    #                 ,NULL AS CREDITS_USED
    #                 ,1.00 as CREDIT_PRICE
    #                 ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
    #                 ,'ACTUAL COMPUTE' AS MEASURE_TYPE
    #         from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
    #         JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
    #         ) as COST {ini_date} group by 5, 1, 2, 6 order by 5 asc
    #         ;
    #
    #     """
    #     return self.query_to_df(sql)

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
        return self.query_to_df(sql)

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
        from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3,6,7
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
        from {self.dbname}.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3
        order by 1 desc;
        """
        return self.query_to_df(sql)

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
                WMH.WAREHOUSE_NAME
                ,WMH.CREDITS_USED_CLOUD_SERVICES as credits
                ,({credit_val}*credits) as dollars
                ,WMH.START_TIME
                ,WMH.END_TIME
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;
        """

        return self.query_to_df(sql)

    def cost_of_cloud_services(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of cloud services for a given time period using Snowflake Warehouse
        Metering History tables. Outputs a dataframe with the following columns:
        WAREHOUSE_NAME: Name of the warehouse
        TOTAL_CREDITS_USED: Total cloud services credits used during a selected billing period
        TOTAL_DOLLARS_USED: Total cost of cloud services credits (in dollars) used during a
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
                WMH.WAREHOUSE_NAME
                ,SUM(WMH.CREDITS_USED_CLOUD_SERVICES) as credits
                ,({credit_val}*credits) as dollars
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
        where WMH.START_TIME between '{start_date}' and '{end_date}' group by 1 order by 1 asc
        """

        return self.query_to_df(sql)

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
                WMH.WAREHOUSE_NAME
                ,WMH.CREDITS_USED_COMPUTE as credits
                ,({credit_val}*credits) as dollars
                ,WMH.START_TIME
                ,WMH.END_TIME
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
        where WMH.START_TIME between '{start_date}' and '{end_date}' order by 4 asc;
        """
        return self.query_to_df(sql)

    def cost_of_compute(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of compute for a given time period using Snowflake Warehouse
        Metering History tables. Outputs a dataframe with the following columns:
        Warehouse Name: Name of the warehouse
        Total credits used: Compute credits used during the billing period
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
                WMH.WAREHOUSE_NAME
                ,sum(WMH.CREDITS_USED_COMPUTE) as credits
                ,({credit_val}*credits) as dollars
        from {self.dbname}.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
        where WMH.START_TIME between '{start_date}' and '{end_date}' group by 1 order by 1 asc
        """
        return self.query_to_df(sql)

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
            ,({credit_val}*credits) as dollars
            ,start_time
            ,end_time
        from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 6 desc;
        """
        return self.query_to_df(sql)

    def cost_of_materialized_views(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of materialized views for a given time
        period using Materialized View Refresh History table.
        Outputs a dataframe with the following columns:
        Database name: The database name of the table
        Schema name: The schema name of the table that was used for materialized views
        Table name: The name of the table that was used for materialized views
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
            ,IFNULL(sum(credits_used), NULL) as credits
            ,IFNULL(({credit_val}*credits), NULL) as dollars
        from {self.dbname}.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1,2,3
        order by 1 asc;
        """
        return self.query_to_df(sql)

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
        sql = f"""
        select
            database_name
            ,credits_used as credits
            ,({credit_val}*credits) as dollars
            ,start_time
            ,end_time
        from {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        order by 4 desc;
        """

        return self.query_to_df(sql)

    def cost_of_replication(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of replication used in a given time
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
        sql = f"""
        select
            database_name
            ,sum(credits_used) as credits
            ,({credit_val}*credits) as dollars
        from {self.dbname}.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        where start_time between '{start_date}' and '{end_date}'
        group by 1
        order by 1 asc;
        """

        return self.query_to_df(sql)

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
             ,sum(credits_used) as credits
             ,({credit_val}*credits) as dollars
         from {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
         where start_time between '{start_date}' and '{end_date}'
         group by 1,2,3
         order by 1 desc;
        """

        return self.query_to_df(sql)

    def cost_of_searchoptimization(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of search optimizations used in a
        given time period using Search Optimization History table.
        Outputs a dataframe with the following columns:
        Database name: The database name of the table on which search optimization is applied
        Schema name: The schema name on which search optimization is applied
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
             database_name
             ,schema_name
             ,table_name
             ,sum(credits_used) as credits
             ,({credit_val}*credits) as dollars
         from {self.dbname}.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY
         where start_time between '{start_date}' and '{end_date}'
         group by 1,2,3
         order by 1 desc;
        """
        return self.query_to_df(sql)

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
            pipe_name
            ,credits_used as credits
            ,start_time
            ,end_time
            ,({credit_val}*credits) as dollars
          from {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
          where start_time between '{start_date}' and '{end_date}'
          order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_snowpipe(self, start_date='2022-01-01', end_date=''):
        """Calculates the overall cost of snowpipe usage in a
        given time period using Pipe Usage History table.
        Outputs a dataframe with the following columns:
        Pipe name: Name of the snowpipe used
        Start time: The start date and time of the billing period
        End time: The end date and time of the billing period
        Credits used: Total credits used during the billing period
        Total dollars used: Total cost of credits (in dollars) used during a
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
            pipe_name
            ,sum(credits_used) as credits
            ,({credit_val}*credits) as dollars
          from {self.dbname}.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
          where start_time between '{start_date}' and '{end_date}'
          group by 1
          order by 1 desc;
        """
        return self.query_to_df(sql)

    def cost_of_storage_ts(self, start_date='2022-01-01', end_date=''):
        """
        Calculates the overall cost of storage usage in time series in a
        given time period using Storage Usage Su table.
        Outputs a dataframe with the following columns:
        Category name: Category name as Storage
        Usage date: The date on which storage is used
        Dollars used: Total cost of storage (in dollars) used
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql = f"""
         select cost.category_name, cost.USAGE_DATE as start_time, cost.DOLLARS_USED as dollars from (
        SELECT
                'Storage' AS category_name
                ,SU.USAGE_DATE
                ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH as DOLLARS_USED
        from    {self.dbname}.ACCOUNT_USAGE.STORAGE_USAGE SU
        JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH
        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM
        GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA
        ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')) as cost
        where cost.usage_date between '{start_date}' and '{end_date}' group by 1, 2, 3 order by 2 asc;
        """
        return self.query_to_df(sql)
