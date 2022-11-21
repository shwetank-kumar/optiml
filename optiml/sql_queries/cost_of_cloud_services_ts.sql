SELECT DISTINCT cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_used, 
sum(SUM(credits_used)) OVER (order by cost.START_TIME ASC) as Cumulative_Credits_Total, cost.START_TIME, cost.END_TIME  from (
SELECT DISTINCT
         'WH Compute' as WAREHOUSE_GROUP_NAME,
         WEH.USER_NAME
        ,WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,WMH.END_TIME
        ,WMH.CREDITS_USED_CLOUD_SERVICES as CREDITS_USED
        --,1.00 as CREDIT_PRICE
        --,(1.00*WMH.CREDITS_USED_CLOUD_SERVICES) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join KIV.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
) as COST group by 4, 1, 5 order by 4 asc
;
