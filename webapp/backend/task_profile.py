from .snflk import SNFLKQuery
from datetime import date

class TaskProfile(SNFLKQuery):
    ## Query cost related queries
    ##TODO: 1) Check query 2) Add flag to give unique query text with parameters
    
    
    def failed_tasks(self,start_date="2022-01-01", end_date=""):
        """
        Returns list of task executions that have failed.
        Ouputs all columns of ACCOUNT_USAGE.TASK HISTORY table.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        select *
        from {self.dbname}.account_usage.task_history
        WHERE STATE = 'FAILED'
        and query_start_time between '{start_date}' and '{end_date}'
        order by query_start_time DESC
        
        """
        df=self.query_to_df(sql)
        return df
    
    def long_running_tasks(self,start_date="2022-01-01", end_date=""):
        """
        Shows an ordered list of the longest running tasks.
        Outputs a dataframe with the following columns:
        DURATION_SECONDS: Number of seconds the task ran.
        and all columns of ACCOUNT_USAGE.TASK_HISTORY table.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        select DATEDIFF(seconds, QUERY_START_TIME,COMPLETED_TIME) as DURATION_SECONDS
        ,*
        from {self.dbname}.account_usage.task_history
        WHERE STATE = 'SUCCEEDED'
        and query_start_time between '{start_date}' and '{end_date}'
        order by DURATION_SECONDS desc
        """
        df=self.query_to_df(sql)
        return df