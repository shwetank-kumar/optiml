SELECT
         'Materialized Views' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,SUM(MVH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by MVH.START_TIME ASC) as Cumulative_Credits_Total
        ,MVH.START_TIME
        ,MVH.END_TIME
        --,1.00 as CREDIT_PRICE
        --,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
        --,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH group by 5, 2, 6 order by 5
