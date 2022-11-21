SELECT
         'Replication' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME AS WAREHOUSE_NAME
        ,SUM(RUH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by RUH.START_TIME ASC) as Cumulative_Credits_Total
        ,RUH.START_TIME
        ,RUH.END_TIME
from    KIV.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH group by 5, 2, 6 order by 5
