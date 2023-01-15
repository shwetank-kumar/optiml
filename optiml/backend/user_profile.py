from .snflk import SNFLKQuery
from datetime import date

class UserProfile(SNFLKQuery):

    ### User queries ---
    
    def idle_users(self, start_date='2022-01-01',end_date=''):
        """
        Shows users that have not logged into account during the time period.
        Outputs a dataframe with the following columns:
        created_on: Date and time  when the user's account was created
        deleted_on: Date and time (in the UTC time zone) when the user's account was deleted.
        login_name: Name that the user enters to log into the system.
        email: Email address for the user.
        must_change_password: Specifies whether the user is forced to change their password on their next login.
        disabled: Specified whether the user account is disabled preventing the user from logging in to the Snowflake and running queries
        snowflake_lock: Specifies whether a temporary lock has been placed on the user's account.
        default_role: The role that is active by default for the user's session upon login.
        last_success_login: Date and time when the user last logged in to the account.
        locked_until_time: Specifies the number of minutes until the temporary lock on the user login is cleared
        password_last_set_time: The timestamp on which the last non-null password was set for the user

        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT
        name
        ,created_on
        ,deleted_on
        ,login_name
        ,email
        ,must_change_password
        ,disabled
        ,snowflake_lock
        ,default_role
        ,last_success_login
        ,locked_until_time
        ,password_last_set_time
        FROM {self.dbname}.ACCOUNT_USAGE.USERS 
        WHERE LAST_SUCCESS_LOGIN < '{start_date}'
        AND DELETED_ON IS NULL
        ORDER BY LAST_SUCCESS_LOGIN ASC
        """
        df=self.query_to_df(sql)
        return df

    def users_never_logged_in(self,start_date="2022-02-02", end_date=""):
        """
        Shows users that have never logged into their account.
        Outputs a dataframe with the following columns:
        created_on: Date and time  when the user's account was created
        deleted_on: Date and time (in the UTC time zone) when the user's account was deleted.
        login_name: Name that the user enters to log into the system.
        email: Email address for the user.
        must_change_password: Specifies whether the user is forced to change their password on their next login.
        disabled: Specified whether the user account is disabled preventing the user from logging in to the Snowflake and running queries
        snowflake_lock: Specifies whether a temporary lock has been placed on the user's account.
        default_role: The role that is active by default for the user's session upon login.
        last_success_login: Date and time when the user last logged in to the account.
        locked_until_time: Specifies the number of minutes until the temporary lock on the user login is cleared
        password_last_set_time: The timestamp on which the last non-null password was set for the user        
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT
        name
        ,created_on
        ,deleted_on
        ,login_name
        ,email
        ,must_change_password
        ,disabled
        ,snowflake_lock
        ,default_role
        ,last_success_login
        ,locked_until_time
        ,password_last_set_time
        FROM {self.dbname}.ACCOUNT_USAGE.USERS 
        WHERE LAST_SUCCESS_LOGIN IS NULL
        AND DELETED_ON IS NULL;
        """
        df=self.query_to_df(sql)
        return df
        
    
    def users_full_table_scans(self, start_date='2022-01-01',end_date='',n=10):
        """
        Shows users that run the most queries with near full table scans.
        Outputs a dataframe with the following columns:
        User_name: User who executed the queries.
        Count_of_queries: Number of queries executed by the user with near full table scans.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT USER_NAME
        ,COUNT(*) as COUNT_OF_QUERIES
        FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME between '{start_date}' and '{end_date}'
        AND PARTITIONS_SCANNED > (PARTITIONS_TOTAL*0.95)
        AND QUERY_TYPE NOT LIKE 'CREATE%'
        group by 1
        order by 2 desc;
        
        """
       
        df=self.query_to_df(sql)
        return df

    def heavy_users(self, start_date='2022-01-01',end_date='',n=10):
        """
        Shows users who run queries that scan a lot of data.
        Outputs a dataframe with the following columns:
        USER_NAME: User who issued the query.
        WAREHOUSE_NAME: Warehouse the query was issued on.
        AVG_PCR_SCANNED: Average partitions scanned by the user. 
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        select 
        User_name
        , warehouse_name
        , avg(case when partitions_total > 0 then partitions_scanned / partitions_total else 0 end) avg_pct_scanned
        from   {self.dbname}.account_usage.query_history
        WHERE START_TIME between '{start_date}' and '{end_date}'
        group by 1, 2
        order by 3 desc"""
        df=self.query_to_df(sql)
        return df
    
    
    def idle_roles(self,start_date="2022-01-01", end_date=""):
        """
        Shows roles that have not been used in the given time period.
        Outputs a dataframe with the following columns:
        ROLE_NAME: Role name that has not been used in the time period.
        
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)

        sql=f"""
        SELECT 
        R.*
        FROM {self.dbname}.ACCOUNT_USAGE.ROLES R
        LEFT JOIN (
            SELECT DISTINCT 
                ROLE_NAME 
            FROM {self.dbname}.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE START_TIME between '{start_date}' and '{end_date}'
                ) Q 
                        ON Q.ROLE_NAME = R.NAME
        WHERE Q.ROLE_NAME IS NULL
        and DELETED_ON IS NULL;
        """
        df=self.query_to_df(sql)
        return df


    def default_user_warehouse(self,start_date="2022-01-01", end_date="",n=3):
        """
        Shows default warehouse associated with a user
        Outputs a dataframe with the following columns:
        default_role: default role associated with a user
        default_warehouse: default warehouse associated with a user.
        """
        if not end_date:
            today_date = date.today()
            end_date = str(today_date)
        sql=f"""
        SELECT
        name
        ,default_warehouse
        ,default_role
        from {self.dbname}.ACCOUNT_USAGE.USERS
        group by 1,2,3
        order by name ASC
        
        
        """
        df=self.query_to_df(sql)
        return df
