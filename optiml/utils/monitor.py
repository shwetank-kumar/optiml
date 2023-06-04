# import sys
# import pathlib

# print(str(pathlib.Path.cwd().parent.parent))
# sys.path.append(str(pathlib.Path.cwd().parent.parent))

import os
import sys

# Get the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate two levels up
parent_dir = os.path.dirname(os.path.dirname(current_dir))

# Add the parent directory to the system path
sys.path.append(parent_dir)

import optiml
from optiml.utils.sf import session, run_sql, sql_to_df


import time
def monitor(target_wh, using_wh, continuous):
    
    def show():
        session.sql(f"USE WAREHOUSE {using_wh}")
        show_df = sql_to_df("show warehouses")

        queries = f"""
        select 
            query_id, query_text, warehouse_name, warehouse_type, cluster_number, execution_status
    from table(information_schema.query_history_by_warehouse('{target_wh}'))
    --where execution_status = 'RUNNING'
    order by start_time desc
    limit 10;
        """
        show_df = sql_to_df("show warehouses")
        cols = ['name', 'state', 'type', 'min_cluster_count', 'max_cluster_count', 'started_clusters', 'running', 'queued']
        subset = show_df[show_df.name == target_wh.upper()][cols]
        print(subset.to_string())

        queries_df = sql_to_df(queries)
        print(queries_df.to_string())
    
    if continuous:
        while(True):
            show()
            time.sleep(1)
    else:
        show()
    # using_conn.close()

            

        
if __name__ == "__main__":
    
    continuous = True
    monitor(target_wh = "test_01c9ec07_356e_4e16_87a4_948225c6f151",using_wh= "demo", continuous=continuous)