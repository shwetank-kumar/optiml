select database_name
      ,schema_name
      ,table_name
      ,sum(credits_used) as credits_used
      ,start_time
      ,end_time
from "KIV"."ACCOUNT_USAGE"."AUTOMATIC_CLUSTERING_HISTORY"
where start_time >= '2020-01-01'
group by 1,2,3,5,6
order by 5 desc;
