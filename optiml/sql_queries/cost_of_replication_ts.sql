select
    database_name
    ,credits_used
    ,start_time
    ,end_time
from "KIV"."ACCOUNT_USAGE"."REPLICATION_USAGE_HISTORY"
where start_time between '2022-05-01' and '2022-05-31'
order by 3 desc;
