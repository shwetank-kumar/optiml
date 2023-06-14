----------------------------------------------------------------
-- Welcome to our setup script. We'll create:
-- * a role/user with minimal *read-only* access to usage data, warehouses, and resource monitors
-- * a scratch database that the role can write to
-- * a secure share that shares this database with our account, so we can perform 
--   deeper analytics without consuming your compute :)
----------------------------------------------------------------

use role accountadmin;

----------------------------------------------------------------
-- set desired values for our role, user, database, and secure share here
----------------------------------------------------------------

set role_name = 'optiml_role'; 
set user_name = 'optiml'; 
set password = '<some_pw>';
set warehouse_name = '<fill me in>';
set db_name = 'optiml';
set db_role = 'optiml_db_role';
set schema_name = $db_name || '.public';
set share_name = 'optiml_share';


----------------------------------------------------------------------
-- the rest of the script is automatic; don't touch any more values :)
----------------------------------------------------------------------
set partner_account = 'NB87219';
--------------------------------
-- create a new role
--------------------------------
create or replace role identifier($role_name);
grant role identifier($role_name) to role accountadmin;

-----------------------------------------------
-- grant it read access to snowflake usage metadata
-----------------------------------------------
grant imported privileges on database snowflake to role identifier($role_name);

--------------------------------
-- allow it to use a warehouse
--------------------------------
grant usage on warehouse identifier($warehouse_name) to role identifier($role_name);

--------------------------------
-- allow it to write to a schema
--------------------------------
create database identifier($db_name);
grant usage on database identifier($db_name) to role identifier($role_name);
grant all on schema identifier($schema_name) to role identifier($role_name);

-----------------------------------------------
-- securely share that schema with our account
-----------------------------------------------
create or replace share identifier($share_name);
create database role identifier($db_role);
grant usage on database identifier($db_name) to database role identifier($db_role);
grant usage on schema identifier($schema_name) to database role identifier($db_role);
grant usage on database identifier($db_name) to share identifier($share_name);
grant database role identifier($db_role) to share identifier($share_name);
alter share identifier($share_name) add accounts=identifier($partner_account);

-----------------------------------------------
-- create a dedicated user with our new role
-----------------------------------------------
create or replace user identifier($user_name)
    password=$password
    must_change_password = true
    default_warehouse = $warehouse_name
    default_role = $role_name
    default_namespace = $db_name
;
grant role identifier($role_name) to user identifier($user_name);

-----------------------------------------------
-- grant the role access to warehouses and resource monitors.
-- note: we won't actually *use* these warehouses; snowflake requires these grants just to inspect their setup.
-----------------------------------------------
show warehouses;
create or replace temp table warehouses as select * from table(result_scan(last_query_id()));
show resource monitors;
create or replace temp table resource_monitors as select * from table(result_scan(last_query_id()));

create temp table setup as
select 'grant usage on warehouse '          || "name" || ' to role ' || $role_name as sql_command from warehouses
union
select 'grant monitor on resource monitor ' || "name" || ' to role ' || $role_name as sql_command from resource_monitors;

-- view all the SQL we're about to run!
select * from setup;


create or replace procedure run_batch_sql(sqlCommand String)
    returns string
    language JavaScript
as
$$
/**
 * Stored procedure to execute multiple SQL statements generated from a SQL query
 */
      cmd1_dict = {sqlText: SQLCOMMAND};
      stmt = snowflake.createStatement(cmd1_dict);
      rs = stmt.execute();
      var s = '';
      while (rs.next())  {
          cmd2_dict = {sqlText: rs.getColumnValue("SQL_COMMAND")};
          stmtEx = snowflake.createStatement(cmd2_dict);
          stmtEx.execute();
          s += rs.getColumnValue(1) + "\n";
          }
           
      return s;
       
$$
;

call run_batch_sql('select * from setup');

-----------------------------------------------------------------
-- almost done: test that the setup worked; should run without error!
-----------------------------------------------------------------
use role identifier($role_name);
use warehouse identifier($warehouse_name);
create or replace table setup_success as select * from snowflake.account_usage.warehouse_metering_history limit 10;
grant select on table setup_success to database role identifier($db_role);
use role accountadmin;

-----------------------------------------------------------------
-- finally, send us the credential parameters; 
-- just copy/paste the results of this query.
-----------------------------------------------------------------
show variables;
select "name", "value" from table(result_scan(last_query_id()))
union 
select 'account' name, (select current_account()) as value;