select
    database_name
    ,schema_name
    ,table_name
    ,sum(credits_used) as credits_used

from "KIV"."ACCOUNT_USAGE"."MATERIALIZED_VIEW_REFRESH_HISTORY"
where start_time between '2022-05-01' and '2022-05-31'
group by 1,2,3,4
order by 5 desc
;
