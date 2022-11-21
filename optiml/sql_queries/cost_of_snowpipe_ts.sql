SELECT
         'Snowpipe' AS WAREHOUSE_GROUP_NAME
        ,PUH.PIPE_NAME AS WAREHOUSE_NAME
        ,SUM(PUH.CREDITS_USED) as credits_used
        ,sum(SUM(credits_used)) OVER (order by PUH.START_TIME ASC) as Cumulative_Credits_Total
        ,PUH.START_TIME
        ,PUH.END_TIME
        --,1.00 as CREDIT_PRICE
        --,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
        --,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH group by 5, 2, 6 order by 5
