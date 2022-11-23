select database_name
      ,schema_name
      ,table_name
      ,sum(credits_used) as total_credits_used
from "KIV"."ACCOUNT_USAGE"."AUTOMATIC_CLUSTERING_HISTORY"
where start_time >= '2020-01-01'
group by 1,2,3
order by 1 desc;
