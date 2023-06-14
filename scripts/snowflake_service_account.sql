// use sysadmin
create schema {optiml_share_prod}.optiml;

// use securityadmin
grant usage on schema {optiml_share_prod}.optiml to role {generic_admin_role};

grant all on schema {optiml_share_prod}.optiml to role {generic_admin_role};
grant select on future views in schema {optiml_share_prod}.optiml to role {generic_admin_role};
grant select on future tables in schema {optiml_share_prod}.optiml to role {generic_admin_role};

// use accountadmin
show views in snowflake.account_usage;

-- create secure views for all views in account_usage schema
create or replace secure view {optiml_share_prod}.optiml.ACCESS_HISTORY as select * from snowflake.account_usage.ACCESS_HISTORY;
...
create or replace secure view {optiml_share_prod}.optiml.WAREHOUSE_METERING_HISTORY as select * from snowflake.account_usage.WAREHOUSE_METERING_HISTORY;

// use role securityadmin
create role optiml_role;
grant usage on database {optiml_share_prod} to role optiml_role;
grant usage on schema {optiml_share_prod}.optiml to role optiml_role;
grant usage on warehouse {optiml_warehouse} to role optiml_role;
grant select on all views in schema {optiml_share_prod}.optiml to role optiml_role;

grant role optiml_role to role {generic_admin_role};
grant resource monitor on account to role {generic_admin_role};

show grants to role optiml_role;

// use role sysadmin
create user if not exists optiml
    password='<some_pw>'
    must_change_password = TRUE
    default_warehouse = 'PROD_WH'
    default_role = 'OPTIML_ROLE'
;

// use role securityadmin
grant role optiml_role to user optiml;