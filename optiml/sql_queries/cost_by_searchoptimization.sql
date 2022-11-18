SELECT
         'Search Optimization' AS WAREHOUSE_GROUP_NAME
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,NULL AS GROUP_CONTACT
        ,NULL AS GROUP_COST_CENTER
        ,NULL AS GROUP_COMMENT
        ,SOH.START_TIME
        ,SOH.END_TIME
        ,SOH.CREDITS_USED
        ,1.00
        ,(1.00*SOH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY SOH
