SHOW WAREHOUSES;
select * from table(result_scan(last_query_id())) where "auto_suspend" is NULL;
