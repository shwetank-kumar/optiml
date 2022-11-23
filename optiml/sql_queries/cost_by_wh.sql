select warehouse_name
      ,sum(credits_used) as credits_used_compute_sum
from account_usage.warehouse_metering_history
where start_time between '2022-05-01' and '2022-05-31' -->= dateadd(day, -5, current_timestamp())  -- Past m days
group by 1
order by 2 desc;
