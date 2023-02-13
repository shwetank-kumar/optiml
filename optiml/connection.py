import configparser
import logging
from pathlib import Path

import snowflake.connector


class SnowflakeConnConfig:
    # mandatory and optional args mapping from their name in snowsql (and command line) args
    # to their corresponding Connector arg name.
    CONN_ARGNAME_MAP = {"accountname" : "account",
                        "username"    : "user",
                        "password"    : "password",
                        "region"       : "region",
                        "warehousename": "warehouse",
                        "rolename"     : "role",
                        "proxy_host"   : "proxy_host",
                        "proxy_port"   : "proxy_port",
                        "authenticator": "authenticator",
                        }
    MANDATORY_ARGNAMES = {"accountname", "username"}

    def __init__(self, fallback_to_snowsql_config=True, **conn_args):
        illegal_args = conn_args.keys() - self.CONN_ARGNAME_MAP.keys()
        # Validate conn arg names
        if illegal_args:
            raise ValueError(f"Invalid connection config arguments: {illegal_args}")

        # check for missing mandatory args and fill in from snowsql config, if allowed.
        config = conn_args.copy()
        mandatory_args = self.MANDATORY_ARGNAMES
        if "authenticator" not in config: # With authenticator="externalbrowser", password can be dropped
            mandatory_args |= {"password"}
        missing_args = self.MANDATORY_ARGNAMES - config.keys()
        if missing_args and fallback_to_snowsql_config:
            for k,v in self.get_snowsql_account_config().items():
                config.setdefault(k, v)
        missing_args = self.MANDATORY_ARGNAMES - config.keys()
        if missing_args:
            raise ValueError(f"Missing the following required connection arguments: {missing_args}")
        self._config = config


    @property
    def config(self):
        return self._config


    @classmethod
    def get_snowsql_account_config(cls):
        '''Get credentials from an existing standards installation of snowsql. Return as a dict'''
        cnfg = {}
        snowsql_config_fn = Path("~/.snowsql/config").expanduser().resolve()
        # print(snowsql_config_fn)
        if snowsql_config_fn.exists():
            logging.info(f"Looking up credentials in {snowsql_config_fn}")
            config = configparser.ConfigParser()
            config.read(snowsql_config_fn)
            conn = config['connections']
            for k in cls.CONN_ARGNAME_MAP.keys():
                try:
                    cnfg[k] = conn[k].strip(" \"'")
                except KeyError:
                    pass
        if len(cnfg) == 0:
            raise ValueError("failed to resolve credentials from snowsql config")
        return cnfg


    def create_connection(self):
        conn_args = {self.CONN_ARGNAME_MAP[k] : v
                     for k,v in self.config.items()}
        connection_obj = snowflake.connector.connect(**conn_args)
        print("Connected to Snowflake.")
        return connection_obj
