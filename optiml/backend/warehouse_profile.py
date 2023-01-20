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

    def count_of_queries_wh(self,start_date="2022-01-01", end_date="",wh_name='DEV_WH'):
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT COUNT(*) AS QUERY_COUNT,
        WAREHOUSE_NAME,
        date_trunc('hour', start_time) as hourly_start_time
        from {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY
        where warehouse_name='{wh_name}'
        and start_time between '{start_date}' and '{end_date}'
        group by warehouse_name,hourly_start_time
        order by hourly_start_time ASC
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
        SELECT DATE_TRUNC('{delta}', wl.start_time) start_time_truncated,
        AVG(avg_running) avg_running, AVG(avg_queued_load) avg_queued_load 
        FROM {self.dbname}.account_usage.warehouse_load_history wl
        WHERE DATE_TRUNC('DAY', wl.start_time) between'{start_date}' and '{end_date}'
        AND wl.warehouse_name = '{wh_name}'
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
          WHERE DATE_TRUNC('day', wm.start_time) between '{start_date}' and '{end_date}'
            AND wm.warehouse_name = '{wh_name}'
            
        ORDER BY start_time_truncated ASC)
        SELECT wlh.*, wmh.credits_used
        FROM   wlh, wmh
        WHERE  wlh.start_time_truncated = wmh.start_time_truncated
                
        """
        df=self.query_to_df(sql)
        return df








