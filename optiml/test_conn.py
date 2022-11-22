import configparser
import logging
from pathlib import Path
from snowflake import connector

class SnowflakeConnConfig:
    # mandatory and optional args mapping from their name in snowsql (and command line) args
    # to their corresponding Connector arg name.
    CONN_ARGNAME_MAP = {"accountname" : "account",
                        "username"    : "username",
                        "password"    : "password",
                        "region"       : "region",
                        "warehousename": "warehouse",
                        "rolename"     : "role",
                        "proxy_host"   : "proxy_host",
                        "proxy_port"   : "proxy_port",
                        "authenticator": "authenticator",
                        }
    MANDATORY_ARGNAMES = {"accountname", "username"}

    # def __init__(self, fallback_to_snowsql_config=True, **conn_args):
    #     illegal_args = conn_args.keys() - self.CONN_ARGNAME_MAP.keys()
    #     # Validate conn arg names
    #     if illegal_args:
    #         raise ValueError(f"Invalid connection config arguments: {illegal_args}")
    #
    #     # check for missing mandatory args and fill in from snowsql config, if allowed.
    #     config = conn_args.copy()
    #     mandatory_args = self.MANDATORY_ARGNAMES
    #     if "authenticator" not in config: # With authenticator="externalbrowser", password can be dropped
    #         mandatory_args |= {"password"}
    #     missing_args = self.MANDATORY_ARGNAMES - config.keys()
    #     if missing_args and fallback_to_snowsql_config:
    #         for k,v in self.get_snowsql_account_config().items():
    #             config.setdefault(k, v)
    #     missing_args = self.MANDATORY_ARGNAMES - config.keys()
    #     if missing_args:
    #         raise ValueError(f"Missing the following required connection arguments: {missing_args}")
    #     self._config = config
    #
    #
    # @property
    # def config(self):
    #     return self._config
    #
    #
    # @classmethod
    # def get_snowsql_account_config(cls):
    #     '''Get credentials from an existing standards installation of snowsql. Return as a dict'''
    #     cnfg = {}
    #     snowsql_config_fn = Path("~/.snowsql/config").expanduser().resolve()
    #     print(snowsql_config_fn)
    #     if snowsql_config_fn.exists():
    #         logging.info(f"Looking up credentials in {snowsql_config_fn}")
    #         config = configparser.ConfigParser()
    #         config.read(snowsql_config_fn)
    #         conn = config['connections']
    #         for k in cls.CONN_ARGNAME_MAP.keys():
    #             try:
    #                 cnfg[k] = conn[k].strip(" \"'")
    #             except KeyError:
    #                 pass
    #     if len(cnfg) == 0:
    #         raise ValueError("failed to resolve credentials from snowsql config")
    #     return cnfg


    def create_connection(self):
        print("Connecting...")
        # conn_args = {self.CONN_ARGNAME_MAP[k] : v}
                     # for k,v in self.config.items()}
        conn = connector.connect(user="user", account="account", password="password",database="database",schema="schema")
        try:
            cur = conn.cursor()
            query = """SELECT DISTINCT cost.USER_NAME, cost.WAREHOUSE_GROUP_NAME, SUM(cost.CREDITS_USED) as credits_consumed, TIMESTAMPDIFF('hour', cost.START_TIME, cost.END_TIME) as duration, cost.START_TIME  from (
SELECT DISTINCT
         'WH Compute' as WAREHOUSE_GROUP_NAME,
         WEH.USER_NAME
        ,WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,WMH.END_TIME
        ,WMH.CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,(1.00*WMH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE                   
from    KIV.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH inner join KIV.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WEH on WMH.WAREHOUSE_ID = WEH.WAREHOUSE_ID
UNION
--COMPUTE FROM SNOWPIPE
SELECT
         'Snowpipe' AS WAREHOUSE_GROUP_NAME,
         'USER' as user_name
        ,PUH.PIPE_NAME AS WAREHOUSE_NAME
        ,PUH.START_TIME
        ,PUH.END_TIME
        ,PUH.CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,(1.00*PUH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH



UNION



--COMPUTE FROM CLUSTERING
SELECT
         'Auto Clustering' AS WAREHOUSE_GROUP_NAME,
         'USER' as user_name
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,ACH.START_TIME
        ,ACH.END_TIME
        ,ACH.CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,(1.00*ACH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH



UNION



--COMPUTE FROM MATERIALIZED VIEWS
SELECT
         'Materialized Views' AS WAREHOUSE_GROUP_NAME,
         'USER' AS user_name
        ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
        ,MVH.START_TIME
        ,MVH.END_TIME
        ,MVH.CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,(1.00*MVH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
UNION
SELECT
         'Replication' AS WAREHOUSE_GROUP_NAME,
         'USER' as user_name
        ,DATABASE_NAME AS WAREHOUSE_NAME
        ,RUH.START_TIME
        ,RUH.END_TIME
        ,RUH.CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,(1.00*RUH.CREDITS_USED) AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH



UNION



--STORAGE COSTS
SELECT
         'Storage' AS WAREHOUSE_GROUP_NAME,
         'USER' as user_name
        ,'Storage' AS WAREHOUSE_NAME
        ,SU.USAGE_DATE
        ,SU.USAGE_DATE
        ,NULL AS CREDITS_USED
        ,1.00 as CREDIT_PRICE
        ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
        ,'ACTUAL COMPUTE' AS MEASURE_TYPE
from    KIV.ACCOUNT_USAGE.STORAGE_USAGE SU
JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
) as COST group by 5, 1, 2, 4 order by 5 asc
;"""
            result = cur.execute(query)
            for i in result:
                print(i)

        except Exception as e:
            print(e)

        finally:
            conn.close()

obj = SnowflakeConnConfig()
print(obj.create_connection())
