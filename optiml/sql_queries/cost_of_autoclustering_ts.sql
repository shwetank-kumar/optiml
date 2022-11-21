SELECT
        'Auto Clustering' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,sum(ACH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by ACH.START_TIME ASC) as Cumulative_Credits_Total
        ,ACH.START_TIME
        ,ACH.END_TIME
        --,1.00 as CREDIT_PRICE
        --,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
        --,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH group by 5, 2, 6 order by 5
