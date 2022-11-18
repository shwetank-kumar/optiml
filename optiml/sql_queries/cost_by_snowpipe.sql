SELECT
         'Snowpipe' AS WAREHOUSE_GROUP_NAME,
         'USER' as user_name
        ,PUH.PIPE_NAME AS WAREHOUSE_NAME
        ,PUH.START_TIME
        ,PUH.END_TIME
        ,PUH.CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH
