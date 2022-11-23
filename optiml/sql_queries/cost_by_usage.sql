with cte_date_wh as(
  select
      warehouse_name
      ,sum(credits_used) as credits_used_date_wh
      ,start_time
      ,end_time
  from KIV.account_usage.warehouse_metering_history
  group by start_time
      ,warehouse_name,end_time
)
select
      warehouse_name
      ,sum(credits_used_date_wh)
      --,start_time
      --,end_time
from cte_date_wh where start_time between '2022-05-01' and '2022-05-31' group by warehouse_name;
