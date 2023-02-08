// use sysadmin
create schema kiva_prod.optiml;

// use securityadmin
grant usage on schema kiva_prod.optiml to role vertex_admin_role_prod;

grant all on schema kiva_prod.optiml to role vertex_admin_role_prod;
grant select on future views in schema kiva_prod.optiml to role vertex_admin_role_prod;
grant select on future tables in schema kiva_prod.optiml to role vertex_admin_role_prod;

// use accountadmin
show views in snowflake.account_usage;

-- create secure views
...
create or replace secure view kiva_prod.optiml.WAREHOUSE_METERING_HISTORY as select * from snowflake.account_usage.WAREHOUSE_METERING_HISTORY;

// use vertex_admin_role_prod
-- test
select * from kiva_prod.optiml.query_history limit 100;

-- create test object
create or replace table kiva_prod.optiml.kiva_optiml_share_test as 

    select 'foo' as col_a, 0 as col_b
    union
    select 'bar' as col_a, 1 as col_b
;

-- use ACCOUNTADMIN < PICK UP HERE
create share optiml_kiva_share;

grant usage on database kiva_prod to share optiml_kiva_share;
grant usage on schema kiva_prod.optiml to share optiml_kiva_share;

grant select on view kiva_prod.optiml.kiva_optiml_share_test to share optiml_kiva_share;

show grants to share optiml_kiva_share;

alter share optiml_kiva_share add accounts=VUJSGPR.CMB57438;

alter session set simulated_data_sharing_consumer = xy12345;
