from .snflk import SNFLKQuery
from datetime import date

class WarehouseProfile(SNFLKQuery):

    ## Config related queries
    def warehouse_config(self):
        """Gives the details of the wareouse config"""
        sql = f"""select * from {self.dbname}.warehouses"""
        df = self.query_to_df(sql)
        return df
    
    def wh_names(self,start_date="2022-01-01", end_date=""):
        """Gives the wareouse names"""
        sql = f"""select distinct warehouse_name 
                from {self.dbname}.warehouse_metering_history
                where date_trunc('hour', start_time) between'{start_date}' and '{end_date}';
            """
        df = self.query_to_df(sql)
        return df


    def wh_load_and_efficiency(self,start_date="2022-01-01", end_date="", warehouse_name="warehouse_name",delta='hour'):
        """"
        Displays avg_running_load, avg_queued_load,hourly_start_time for given warehouse in a given time period from warehouse load history table.
        Displays count of queries, average execution time, average provisioning time, average compilation time, average queued overload time
        for queries running in a given time period for given warehouse from query history table.
        Displays credits used by given warehouse in tim period from warehouse metering table.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)

        sql = f"""
        WITH wlh as (
            SELECT DATE_TRUNC('{delta}', wl.start_time) hourly_start_time,
            warehouse_name,
            AVG(avg_running) avg_running_load, 
            AVG(avg_queued_load) avg_queued_load
            FROM {self.dbname}.warehouse_load_history wl
            WHERE DATE_TRUNC('{delta}', wl.start_time) between'{start_date}' and '{end_date}'
            and warehouse_name='{warehouse_name}'
            GROUP BY  hourly_start_time, warehouse_name
            ORDER BY hourly_start_time asc
        ),
        wmh AS (
            SELECT DATE_TRUNC('{delta}', wm.start_time) hourly_start_time, 
            warehouse_name,
            round(avg(credits_used),2) as avg_credits
            FROM {self.dbname}.warehouse_metering_history wm
            WHERE DATE_TRUNC('{delta}', wm.start_time) between '{start_date}' and '{end_date}'
            and warehouse_name='{warehouse_name}'
            GROUP BY  hourly_start_time, warehouse_name
            ORDER BY hourly_start_time asc
        ),
        qh as (
            SELECT DATE_TRUNC('{delta}', qh.start_time) hourly_start_time, 
            warehouse_name,
            COUNT(*) query_count,
            AVG(compilation_time) avg_compilation_time,
            AVG(execution_time) avg_execution_time,
            AVG(queued_provisioning_time) avg_queued_provisioning_time,
            AVG(queued_repair_time) avg_queued_repair_time,
            AVG(queued_overload_time) avg_queued_overload_time
            FROM {self.dbname}.query_history qh
            WHERE DATE_TRUNC('{delta}', qh.start_time) between '{start_date}' and '{end_date}'
            and warehouse_name='{warehouse_name}'
            GROUP BY  hourly_start_time, warehouse_name
            ORDER BY  hourly_start_time
        )
        SELECT wlh.hourly_start_time, 
        wlh.warehouse_name,
        wlh.avg_running_load, 
        wlh.avg_queued_load,
        wmh.avg_credits,
        round(wlh.avg_running_load / (wmh.avg_credits + 0.001) * 100,2) as avg_efficiency, 
        qh.query_count,
        qh.avg_compilation_time,
        qh.avg_execution_time,
        qh.avg_queued_overload_time
        FROM wlh, wmh, qh
        WHERE wlh.hourly_start_time = wmh.hourly_start_time
        AND qh.hourly_start_time = wmh.hourly_start_time
        AND wlh.warehouse_name = wmh.warehouse_name
        AND qh.warehouse_name = wmh.warehouse_name
        """
        df=self.query_to_df(sql)
        return df


    # def wh_queued_load_ts(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH',delta='minute'):
    #     """"
    #     Displays avg_running_load, avg_queued_load,hourly_start_time for given warehouse in a given time period from warehouse load history table.
    #     Displays count of queries, average execution time, average provisioning time, average compilation time, average queued overload time
    #     for queries running in a given time period for given warehouse from query history table.
    #     Displays credits used by given warehouse in tim period from warehouse metering table.
    #     """
    #     if not end_date:
    #         today_date = date.today()
    #         end_date = str(today_date)

    #     sql = f"""
    #     WITH wlh as (
    #     SELECT DATE_TRUNC('{delta}', wl.start_time) hourly_start_time,
    #     AVG(avg_running) avg_running_load, 
    #     AVG(avg_queued_load) avg_queued_load
    #     FROM {self.dbname}.warehouse_load_history wl
    #     WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
    #     AND wl.warehouse_name = '{wh_name}'
    #     GROUP BY  hourly_start_time
    #     ORDER BY hourly_start_time asc
    #     ),
    #     wmh AS (
    #         SELECT DATE_TRUNC('{delta}', wm.start_time) hourly_start_time, 
    #         round(avg(credits_used),2) as avg_credits
    #        FROM {self.dbname}.warehouse_metering_history wm
    #       WHERE DATE_TRUNC('DAY', wm.start_time) between '{start_date}' and '{end_date}'
    #         AND wm.warehouse_name = '{wh_name}'
    #         GROUP BY  hourly_start_time
    #         ORDER BY hourly_start_time asc
    #     ),
    #     qh as (
    #     SELECT DATE_TRUNC('{delta}', qh.start_time) hourly_start_time, 
    #     COUNT(*) query_count,
    #     AVG(compilation_time) avg_compilation_time,
    #     AVG(execution_time) avg_execution_time,
    #     AVG(queued_provisioning_time) avg_queued_provisioning_time,
    #     AVG(queued_repair_time) avg_queued_repair_time,
    #     AVG(queued_overload_time) avg_queued_overload_time
    #     FROM {self.dbname}.query_history qh
    #     WHERE DATE_TRUNC('DAY', qh.start_time) between '{start_date}' and '{end_date}'
    #     AND qh.warehouse_name = '{wh_name}'
    #     GROUP BY  hourly_start_time
    #     ORDER BY  hourly_start_time
    #     )
    #     SELECT wlh.hourly_start_time, 
    #     wlh.avg_running_load, 
    #     wlh.avg_queued_load,
    #     wmh.avg_credits,
    #     round(wlh.avg_running_load / (wmh.avg_credits + 0.001) * 100,2) as avg_efficiency, 
    #     qh.query_count,
    #     qh.avg_compilation_time,
    #     qh.avg_execution_time,
    #     qh.avg_queued_overload_time
    #     FROM wlh, wmh, qh
    #     WHERE wlh.hourly_start_time = wmh.hourly_start_time
    #     AND qh.hourly_start_time = wmh.hourly_start_time
    #     """
    #     df=self.query_to_df(sql)
    #     return df

    # def wh_queued_load_ts(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH',delta='minute'):
    #     """"
    #     Displays avg_running_load, avg_queued_load,hourly_start_time for given warehouse in a given time period from warehouse load history table.
    #     Displays count of queries, average execution time, average provisioning time, average compilation time, average queued overload time
    #     for queries running in a given time period for given warehouse from query history table.
    #     Displays credits used by given warehouse in tim period from warehouse metering table.
    #     """
    #     if not end_date:
    #         today_date = date.today()
    #         end_date = str(today_date)
    #     sql=f"""
        
    #     WITH wlh as (
    #     SELECT DATE_TRUNC('{delta}', wl.start_time) hourly_start_time,
    #     AVG(avg_running) avg_running_load, AVG(avg_queued_load) avg_queued_load 
    #     FROM {self.dbname}.warehouse_load_history wl
    #     WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
    #     AND wl.warehouse_name = '{wh_name}'
    #     GROUP BY  hourly_start_time
    #     ORDER BY hourly_start_time asc
    #     ),
    #     wmh AS (SELECT DATE_TRUNC('{delta}', wm.start_time) hourly_start_time, credits_used
    #        FROM {self.dbname}.warehouse_metering_history wm
    #       WHERE DATE_TRUNC('DAY', wm.start_time) between '{start_date}' and '{end_date}'
    #         AND wm.warehouse_name = '{wh_name}'
    #         ORDER BY hourly_start_time ASC
    #     ),
    #     qh as (
    #     SELECT DATE_TRUNC('{delta}', qh.start_time) hourly_start_time, 
    #     COUNT(*) query_count,
    #     AVG(compilation_time) avg_compilation_time,
    #     AVG(execution_time) avg_execution_time,
    #     AVG(queued_provisioning_time) avg_queued_provisioning_time,
    #     AVG(queued_repair_time) avg_queued_repair_time,
    #     AVG(queued_overload_time) avg_queued_overload_time
    #     FROM {self.dbname}.query_history qh
    #     WHERE DATE_TRUNC('DAY', qh.start_time) between '{start_date}' and '{end_date}'
    #     AND qh.warehouse_name = '{wh_name}'
    #     GROUP BY  hourly_start_time
    #     ORDER BY  hourly_start_time
    #     )
    #     SELECT wlh.hourly_start_time, 
    #     wlh.avg_running_load, 
    #     wlh.avg_queued_load, 
    #     wmh.credits_used, 
    #     qh.query_count,
    #     qh.avg_compilation_time,
    #     qh.avg_execution_time,
    #     qh.avg_queued_overload_time
    #     FROM wlh, wmh, qh
    #     WHERE wlh.hourly_start_time = wmh.hourly_start_time
    #     AND qh.hourly_start_time = wmh.hourly_start_time
    #     """
    #     df=self.query_to_df(sql)
    #     return df
    
    # def wh_efficiency(self, start_datetime="2022-10-01", end_datetime="2022-10-02", warehouse_name='PROD_WH'):
    #     sql = f"""
    #         with cte as (
    #         select date_trunc('second', start_time) as start_time, end_time, warehouse_name, credits_used
    #         from {self.dbname}.warehouse_metering_history
    #         where warehouse_name = '{warehouse_name}'
    #         and date_trunc('hour', start_time) between '{start_datetime}' and '{end_datetime}')
    #         select date_trunc('second', a.start_time) as start_time, 
    #         round(avg(AVG_RUNNING),2) as avg_running, 
    #         round(avg(credits_used),2) as avg_credits, 
    #         round(avg(AVG_RUNNING) / avg(credits_used) * 100,2) as avg_efficiency
    #         from {self.dbname}.warehouse_load_history a
    #         join cte b on a.start_time = date_trunc('hour', a.start_time)
    #         where a.warehouse_name = '{warehouse_name}'
    #         and date_trunc('second', a.start_time) between '{start_datetime}' and '{end_datetime}'
    #         group by 1
    #         order by 1;
    #     """
        
    #     df = self.query_to_df(sql)
    #     df["warehouse_name"] = warehouse_name
        
    #     return df
    

    def wh_analysis(self,start_date="2022-01-01", end_date="",delta='hour',wh_name='DEV_WH',n=2):
        """
        Displays hourly start time and average queued load for average queued load value greater than inputted threshold.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT DATE_TRUNC('{delta}', wl.start_time) hourly_start_time, AVG(avg_queued_load) as avg_queued_load
        FROM {self.dbname}.warehouse_load_history wl
        WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
        AND wl.warehouse_name = '{wh_name}'
        AND wl.avg_queued_load>'{n}'
        GROUP BY  hourly_start_time
        ORDER BY hourly_start_time asc
        """
        df=self.query_to_df(sql)
        return df
    
    def find_queries(self,start_date="2022-01-01", end_date="",delta='hour',wh_name='DEV_WH'):
        # sdate=self.wh_analysis(start_date,end_date,delta,wh_name)
        # sdate=sdate.to_list()
        """
        Displays queries and its relevant data every hour for given time period.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        Select
        qh.query_id,
        DATE_TRUNC('{delta}', qh.start_time) hourly_start_time,
        qh.query_type,
        qh.query_text,
        qh.user_name,
        qh.role_name,
        qh.database_name,
        qh.schema_name,
        qh.warehouse_size,
        qh.warehouse_type,
        qh.bytes_scanned,
        round(qh.percentage_scanned_from_cache*100,2) as percentage_scanned_from_cache,
        qh.bytes_spilled_to_local_storage,
        qh.bytes_spilled_to_remote_storage,
        qh.partitions_total,
        round((qh.total_elapsed_time/(1000)),2) as total_time_elapsed_sec,
        round((qh.compilation_time/(1000)),2) as compilation_time_sec,
        round((qh.execution_time/1000),2) as execution_time_sec,
        round((qh.queued_provisioning_time/1000),2) as queued_provisioning_time_sec,
        round((qh.queued_repair_time/1000),2) as queued_repair_time_sec,
        round((qh.queued_overload_time/1000),2) as queued_overload_time_sec,
        round((qh.list_external_files_time/1000),2) as list_external_files_time_sec,
        qh.query_tag,
        qh.execution_status
        from {self.dbname}.query_history qh 
        WHERE DATE_TRUNC('DAY', qh.start_time) between'{start_date}' and '{end_date}'
        AND qh.warehouse_name = '{wh_name}'
        order by hourly_start_time
        """
        df=self.query_to_df(sql)
        return df
        
# TODO: Query takes time to run for large date ranges - need to find optimal solutiom
    
    def wh_query_load(self,start_date="2022-01-01", end_date="",delta='hour',wh_name='DEV_WH',n=2):
        """
        Displays list of queries and relevant data which have an average queued load warehouse value > inputted threshold
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        wh_analysis=self.wh_analysis(start_date,end_date,delta,wh_name,n)
        stime=wh_analysis["hourly_start_time"].to_list()
        find_queries=self.find_queries(start_date,end_date,delta,wh_name)
        time_column=find_queries.iloc[:,1]
        matched_list = [i for i in stime for num in time_column if num == i]
        df = find_queries[find_queries['hourly_start_time'].isin(matched_list)]
        return df

# WH events over a period of time
    
    def wh_events(self,start_datetime="2022-10-01 12:00:00", end_datetime="2022-10-01 19:00:00", warehouse_name=None):
        """
        Displays all the events on the WAREHOUSE_EVENTS_HISTORY tables in a period of time
        """
        if not end_datetime:
            today_date = date.today()
            end_datetime = str(today_date)

        if not warehouse_name:
            warehouse_name = "warehouse_name"

        sql=f"""
            select *
            from {self.dbname}.warehouse_events_history
            where date_trunc('second', timestamp) between '{start_datetime}' and '{end_datetime}'
            AND warehouse_name={warehouse_name}
        """
        df=self.query_to_df(sql)
        return df

# WH credits per hour
    
    def wh_credits_per_hour(self,start_datetime="2022-10-01", end_datetime="2022-10-02",warehouse_name='PROD_WH'):
        """
        Displays credits consumed per hour by warehouse
        """
        if not end_datetime:
            today_date = date.today()
            end_datetime = str(today_date)

        sql=f"""
            select start_time, credits_used
            from {self.dbname}.warehouse_metering_history
            where date_trunc('hour', start_time) between '{start_datetime}' and '{end_datetime}'
            and warehouse_name='{warehouse_name}'
        """
        df=self.query_to_df(sql)
        return df

    
# WH efficiency

        







    








