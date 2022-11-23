SELECT DISTINCT
        WMH.WAREHOUSE_NAME
        ,WMH.CREDITS_USED_COMPUTE as CREDITS_USED
        ,WMH.START_TIME
        ,WMH.END_TIME
from KIV.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH where WMH.START_TIME between '2022-01-01' and '2022-01-31' order by 3 asc
