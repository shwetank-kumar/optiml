#!/usr/bin/env python
# coding: utf-8


# Adding system path
import sys, pathlib

sys.path.append(str(pathlib.Path.cwd().parent.parent))
from dash import Dash, html, dcc, Input, Output
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tabulate import tabulate

color_scheme = ["red", "blue", "green", "orange", "purple", "brown", "pink", "gray", "olive", "cyan", "darkviolet",
                "goldenrod", "darkgreen", "chocolate", "lawngreen"]

# In[5]:


# Initialize connection to Snowflake and set analysis date
from optiml.connection import SnowflakeConnConfig

connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()
# Initialize local environment
import os

cache_dir = os.path.expanduser('~/data/knot')
# Initialize query library
from ..optiml.backend.cost_profile import CostProfile, get_previous_dates

cqlib = CostProfile(connection, 'KNT', cache_dir, "enterprise")

# sdate = '2022-09-21'
# edate = '2022-10-21'
sdate = '2022-10-11'
edate = '2022-10-21'
