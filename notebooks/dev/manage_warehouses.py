import logging
from dotenv import load_dotenv
import sys
sys.path.append("../../../aero/app/")

from lib.warehouse_manager import WarehouseManager
import os
load_dotenv("../../env/.env") 

if __name__ == '__main__':

    manager1 = WarehouseManager(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        role=os.environ['SNOWFLAKE_ROLE'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse_names=['test_suspend_idle_after_one_minute_63fcffb5_6a18_4e6c_bb59_824a2a1862b2'],
        strategy='suspend_idle_after_one_minute'
        )
    manager1.connect()
    manager1.start()

    manager2 = WarehouseManager(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        role=os.environ['SNOWFLAKE_ROLE'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse_names=['test_minimize_clusters_if_no_queuing_ec3ecce1_6661_4d0d_a93c_f5b678be9a19'],
        strategy='minimize_clusters_if_no_queuing'
        )
    manager2.connect()
    manager2.start()
    
    manager3 = WarehouseManager(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        role=os.environ['SNOWFLAKE_ROLE'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse_names=['test_economy_up_standard_down_e9316185_aea0_4325_ba8c_775c3ad551ed'],
        strategy='economy_up_standard_down'
        )
    manager3.connect()
    manager3.start()
    