SELECT
         'Search Optimization' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,SUM(SOH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by SOH.START_TIME ASC) as Cumulative_Credits_Total
        ,SOH.START_TIME
        ,SOH.END_TIME
from    KIV.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY SOH group by 5, 2, 6 order by 5
