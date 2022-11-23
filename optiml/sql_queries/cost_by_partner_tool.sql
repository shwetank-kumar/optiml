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
    from "KIV"."ACCOUNT_USAGE"."QUERY_HISTORY" qh
    join "KIV"."ACCOUNT_USAGE"."SESSIONS" se on se.session_id = qh.session_id
    where warehouse_name is not null
        and execution_time > 0

-- Change the below filter if you want to look at a longer range than the last 1 month
        and start_time > dateadd(month,-1,current_timestamp())
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
        a.client_application_name
        ,c.warehouse_name
        ,(a.client_hour_execution_time/b.hour_execution_time)*c.credits_used as approximate_credits_used

    from client_hour_execution_cte a
    join hour_execution_cte b  on a.start_time_hour = b.start_time_hour and b.warehouse_name = a.warehouse_name
    join "KIV"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY" c on c.warehouse_name = a.warehouse_name and c.start_time = a.start_time_hour
)

select
    client_application_name
    ,warehouse_name
    ,sum(approximate_credits_used) as approximate_credits_used
from approximate_credits
group by 1,2
order by 3 desc;
