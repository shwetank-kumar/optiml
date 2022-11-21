SHOW WAREHOUSES;
select * from table(result_scan(last_query_id())) where "resource_monitor"='null';
