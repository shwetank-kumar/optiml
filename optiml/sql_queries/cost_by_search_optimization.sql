select
    database_name
    ,schema_name
    ,table_name
    ,sum(credits_used) as credits_used

from "KIV"."ACCOUNT_USAGE"."SEARCH_OPTIMIZATION_HISTORY"

where start_time between '2022-05-01' and '2022-05-31'
group by 1,2,3
order by 1 desc;
