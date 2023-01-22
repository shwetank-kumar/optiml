from .snflk import SNFLKQuery
from datetime import date

class WarehouseProfile(SNFLKQuery):

    ## Config related queries
    def warehouse_config(self):
        """Gives the details of the wareouse config"""
        sql = f"""select * from {self.dbname}.account_usage.warehouses"""
        df = self.query_to_df(sql)
        return df

    def caching_warehouse(self, start_date='2022-01-01',end_date='', n=10):
        """
        Shows percentage of data scanned from the warehouse cache
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT WAREHOUSE_NAME
        ,COUNT(*) AS QUERY_COUNT
        ,SUM(BYTES_SCANNED) AS BYTES_SCANNED
        ,SUM(BYTES_SCANNED*PERCENTAGE_SCANNED_FROM_CACHE) AS BYTES_SCANNED_FROM_CACHE
        ,SUM(BYTES_SCANNED*PERCENTAGE_SCANNED_FROM_CACHE) / SUM(BYTES_SCANNED) AS PERCENT_SCANNED_FROM_CACHE
        FROM "{self.dbname}"."ACCOUNT_USAGE"."QUERY_HISTORY"
        WHERE START_TIME between '{start_date}' and '{end_date}'
        AND BYTES_SCANNED > 0
        GROUP BY 1
        ORDER BY 5
        ;
        """
        df=self.query_to_df(sql)
        return df

    def wh_queued_load_ts(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH',delta='minute'):
        """
        Shows the average queued load value in a given warehouse during a time interval (minute,hour,etc.)
        Outputs a dataframe with the following columns:
        Avg_running: Average running load of warehouse in given time interval.
        Avg_queued_load: Average queued load of warehouse in given time interval.
        query_count: Number of queries issued in warehouse in time interval.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        WITH wlh as (
        SELECT DATE_TRUNC('{delta}', wlh.start_time) start_time_truncated,
        AVG(wlh.avg_running) as avg_running, AVG(wlh.avg_queued_load) as avg_queued_load 
        FROM {self.dbname}.account_usage.warehouse_load_history wlh
        WHERE DATE_TRUNC('DAY', wlh.start_time) between'{start_date}' and '{end_date}'
        AND wlh.warehouse_name = '{wh_name}'
        GROUP BY  start_time_truncated
        ORDER BY start_time_truncated asc
        ),
        qh as (
        SELECT DATE_TRUNC('{delta}', qh.start_time) start_time_truncated,
        COUNT(*) query_count
        FROM {self.dbname}.account_usage.query_history qh
        WHERE DATE_TRUNC('DAY', qh.start_time) between'{start_date}' and '{end_date}'
        AND qh.warehouse_name = '{wh_name}'
        GROUP BY  start_time_truncated
        ORDER BY  start_time_truncated
        )
        SELECT wlh.start_time_truncated, 
        wlh.avg_running, 
        wlh.avg_queued_load, 
        qh.query_count
        FROM wlh,qh
        WHERE wlh.start_time_truncated = qh.start_time_truncated
        """
        df=self.query_to_df(sql)
        return df
    
    def wh_credits(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH',delta='minute'):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        WITH wlh as (
        SELECT DATE_TRUNC('{delta}', wl.start_time) start_time_truncated,
        AVG(avg_running) avg_running, AVG(avg_queued_load) avg_queued_load 
        FROM {self.dbname}.account_usage.warehouse_load_history wl
        WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
        AND wl.warehouse_name = '{wh_name}'
        GROUP BY  start_time_truncated
        ORDER BY start_time_truncated asc
        ),
        wmh AS (SELECT DATE_TRUNC('{delta}', wm.start_time) start_time_truncated, credits_used
           FROM {self.dbname}.account_usage.warehouse_metering_history wm
          WHERE DATE_TRUNC('DAY', wm.start_time) between '{start_date}' and '{end_date}'
            AND wm.warehouse_name = '{wh_name}'
            ORDER BY start_time_truncated ASC)
        SELECT wlh.*, wmh.credits_used
        FROM   wlh, wmh
        WHERE  wlh.start_time_truncated = wmh.start_time_truncated
                
        """
        df=self.query_to_df(sql)
        return df
    
    def wh_metrics(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH',delta='minute'):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        
        WITH wlh as (
        SELECT DATE_TRUNC('{delta}', wl.start_time) start_time_truncated,
        AVG(avg_running) avg_running, AVG(avg_queued_load) avg_queued_load 
        FROM {self.dbname}.account_usage.warehouse_load_history wl
        WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
        AND wl.warehouse_name = '{wh_name}'
        GROUP BY  start_time_truncated
        ORDER BY start_time_truncated asc
        ),
        wmh AS (SELECT DATE_TRUNC('{delta}', wm.start_time) start_time_truncated, credits_used
           FROM {self.dbname}.account_usage.warehouse_metering_history wm
          WHERE DATE_TRUNC('DAY', wm.start_time) between '{start_date}' and '{end_date}'
            AND wm.warehouse_name = '{wh_name}'
            ORDER BY start_time_truncated ASC
        ),
        qh as (
        SELECT DATE_TRUNC('{delta}', qh.start_time) start_time_truncated, 
        COUNT(*) query_count,
        AVG(compilation_time) avg_compilation_time,
        AVG(execution_time) avg_execution_time,
        AVG(queued_provisioning_time) avg_queued_provisioning_time,
        AVG(queued_repair_time) avg_queued_repair_time,
        AVG(queued_overload_time) avg_queued_overload_time
        FROM {self.dbname}.account_usage.query_history qh
        WHERE DATE_TRUNC('DAY', qh.start_time) between '{start_date}' and '{end_date}'
        AND qh.warehouse_name = '{wh_name}'
        GROUP BY  start_time_truncated
        ORDER BY  start_time_truncated
        )
        SELECT wlh.start_time_truncated, 
        wlh.avg_running, 
        wlh.avg_queued_load, 
        wmh.credits_used, 
        qh.query_count,
        qh.avg_compilation_time,
        qh.avg_execution_time,
        qh.avg_queued_overload_time
        FROM wlh, wmh, qh
        WHERE wlh.start_time_truncated = wmh.start_time_truncated
        AND qh.start_time_truncated = wmh.start_time_truncated
        """
        df=self.query_to_df(sql)
        return df

    def wh_analysis(self,start_date="2022-01-01", end_date="",delta='hour',wh_name='DEV_WH',n=2):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT DATE_TRUNC('{delta}', wl.start_time) start_time_truncated, AVG(avg_queued_load) as avg_queued_load
        FROM {self.dbname}.account_usage.warehouse_load_history wl
        WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
        AND wl.warehouse_name = '{wh_name}'
        AND wl.avg_queued_load>'{n}'
        GROUP BY  start_time_truncated
        ORDER BY start_time_truncated asc
        """
        df=self.query_to_df(sql)
        return df
    
    def find_queries(self,start_date="2022-01-01", end_date="",delta='hour',wh_name='DEV_WH'):
        # sdate=self.wh_analysis(start_date,end_date,delta,wh_name)
        # sdate=sdate.to_list()
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        Select
        qh.query_id,
        DATE_TRUNC('{delta}', qh.start_time) start_time_truncated,
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
        from {self.dbname}.account_usage.query_history qh 
        WHERE DATE_TRUNC('DAY', qh.start_time) between'{start_date}' and '{end_date}'
        AND qh.warehouse_name = '{wh_name}'
        order by start_time_truncated
        """
        df=self.query_to_df(sql)
        return df
# TODO: Query takes time to run for large date ranges 
    def wh_query_load(self,start_date="2022-01-01", end_date="",delta='hour',wh_name='DEV_WH',n=2):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        wh_analysis=self.wh_analysis(start_date,end_date,delta,wh_name,n)
        stime=wh_analysis["start_time_truncated"].to_list()
        find_queries=self.find_queries(start_date,end_date,delta,wh_name)
        time_column=find_queries.iloc[:,1]
        matched_list = [i for i in stime for num in time_column if num == i]
        df = find_queries[find_queries['start_time_truncated'].isin(matched_list)]
        return df

    







    








