select warehouse_name
      ,credits_used
      ,start_time
      ,end_time
from account_usage.warehouse_metering_history
where start_time between '2022-05-01' and '2022-05-31' -->= dateadd(day, -5, current_timestamp())  -- Past m days
group by 1,2,3,4
order by 3 asc;
