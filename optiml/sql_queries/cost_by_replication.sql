select
    database_name
    ,sum(credits_used) as credits_used
from "KIV"."ACCOUNT_USAGE"."REPLICATION_USAGE_HISTORY"
where start_time between '2022-05-01' and '2022-05-31'
group by 1
order by 1 asc;
