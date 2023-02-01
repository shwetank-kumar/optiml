import snowflake.connector
from connection import SnowflakeConnConfig
from datetime import datetime
import pandas as pd
from tabulate import tabulate
import plotly.express as px
import plotly.graph_objects as go
color_scheme=["red","blue","green","orange","purple","brown","pink","gray","olive","cyan","darkviolet","goldenrod","darkgreen","chocolate","lawngreen"]
from plotly.subplots import make_subplots
from dateutil.relativedelta import relativedelta
connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp',warehousename="XSMALL_WH").create_connection()
import os
cache_dir = os.path.expanduser('~/data/kiva')
import numpy as np
from backend.cost_profile import CostProfile
from backend.query_profile import QueryProfile
cqlib = CostProfile(connection, 'KNT', cache_dir)
qqlib=QueryProfile(connection, 'KNT', cache_dir)
sdate = '2022-09-12'
edate = '2022-10-12'
print(f"The analysis is carried our for date range {sdate} to {edate}")

# ---- Cost analysis plots----------------------

def get_previous_dates(sdate, edate, date_shift_months):
    sdate_datetime = datetime.strptime(sdate,'%Y-%m-%d')
    prev_sdates_datetime = datetime.strptime(sdate,'%Y-%m-%d') - relativedelta(months=date_shift_months)
    prev_sdates = prev_sdates_datetime.strftime("%Y-%m-%d")
    edate_datetime = datetime.strptime(edate,'%Y-%m-%d')
    prev_edates_datetime = datetime.strptime(edate,'%Y-%m-%d') - relativedelta(months=date_shift_months)
    prev_edates = prev_edates_datetime.strftime("%Y-%m-%d")
    return prev_sdates, prev_edates

def previous_month_cost():
    p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)
    df_prev = cqlib.total_cost_breakdown_ts(p1_sdate, p1_edate)
    df_prev = df_prev.fillna('Unassigned')
    df_prev = df_prev.groupby("category_name").sum("numeric_only").reset_index()
    df_prev.loc[len(df_prev.index)] = ['Total', df_prev['credits'].sum(), df_prev['dollars'].sum()]
    df_by_usage_category_prev = df_prev.round(2)
    df_by_usage_category_prev.rename(columns={'category_name': 'Cost category', 'credits': 'Previous month credits','dollars':'Previous month dollars'}, inplace=True)
    return(df_prev,df_by_usage_category_prev)

def current_month_cost():
    df = cqlib.total_cost_breakdown_ts(sdate, edate)
    df = df.fillna('Unassigned')
    df_current = df.groupby("category_name").sum("numeric_only").reset_index()
    df_current.loc[len(df_current.index)] = ['Total', df_current['credits'].sum(), df_current['dollars'].sum()]
    df_by_usage_category = df_current.round(2)
    df_by_usage_category.rename(columns={'category_name': 'Cost category', 'credits': 'Current month credits','dollars':'Current month dollars'}, inplace=True)
    return(df_current,df_by_usage_category)

def stats():
    df = cqlib.total_cost_breakdown_ts(sdate, edate)
    df_by_category_ts = df.groupby(['category_name','hourly_start_time']).sum('numeric_only').reset_index()
    df_compute = df_by_category_ts[df_by_category_ts["category_name"] == "Compute"].round(2)
    avg_consumption = df_compute.mean(numeric_only=True).round(2)
    avg_consumption = pd.DataFrame({'Category':avg_consumption.index, 'Average Consumption':avg_consumption.values})
    avg_consumption.loc[-1] = ['hourly consumption', np.nan]  # adding a row
    avg_consumption.index = avg_consumption.index + 1  # shifting index
    avg_consumption = avg_consumption.sort_index() 
    avg_consumption = avg_consumption.drop('Category', axis=1)
    max_consumption = df_compute.loc[df_compute['credits'].idxmax()]
    max_consumption.drop("category_name", inplace=True)
    max_consumption = pd.DataFrame({'Category':max_consumption.index, 'Max consumption':max_consumption.values})
    max_consumption = max_consumption.drop('Category', axis=1)
    min_consumption = df_compute.loc[df_compute['credits'].idxmin()]
    min_consumption.drop("category_name", inplace=True)
    min_consumption = pd.DataFrame({'Category':min_consumption.index, 'Min consumption':min_consumption.values})
    stats=pd.concat([min_consumption, max_consumption,avg_consumption], axis=1)
    stats.head()
    return(stats)


def percentage_change():
    df_prev1,df_prev2=previous_month_cost()
    df_current1,df_current2=current_month_cost()
    df_change = pd.DataFrame().assign(category_name=df_prev1["category_name"])
    df_change["percent_change"] = ((df_current1["dollars"] - df_prev1["dollars"])/df_prev1["dollars"]*100).round(2)
    df_change.rename(columns={'percent_change': 'Percentage change',}, inplace=True)
    return(df_change)

def usage_cost():
    df_prev1,df_prev2=previous_month_cost()
    df_current1,df_current2=current_month_cost()
    df_change=percentage_change()
    df_current2=df_current2.iloc[: , 1:]
    df_change=df_change.iloc[: , 1:]
    df_all=pd. concat([df_prev2, df_current2, df_change], axis=1)
    return(df_all)
def usage_category():
    df=cqlib.total_cost_breakdown_ts(sdate,edate)
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(), df_by_usage_category['dollars'].sum()]
    df_by_usage_category = df_by_usage_category.round(2)
    df_by_usage_category.reset_index(inplace=True)
    df_by_usage_category.drop(columns=["index"], inplace=True)
    df_by_usage_category = df_by_usage_category.drop(len(df_by_usage_category)-1) 
    df_by_category_ts = df.groupby(['category_name','hourly_start_time']).sum('numeric_only').reset_index()
    return(df_by_usage_category,df_by_category_ts)

def cost_by_usage_plots():
    df_by_usage_category,df_by_category_ts=usage_category()
    fig1 = make_subplots(
    rows=1, cols=2,
    specs=[[{"type": "pie"},{"type": "pie"}]],
    subplot_titles=("Dollars", "Credits")
    )

    fig1.add_trace(go.Pie(labels=df_by_usage_category['category_name'].tolist(), values=df_by_usage_category['dollars'].tolist(),name="Dollars", rotation=45, marker_colors=color_scheme),row=1,col=1)
    fig1.add_trace(go.Pie(labels=df_by_usage_category['category_name'].tolist(), values=df_by_usage_category['credits'].tolist(),name='Credits', rotation=45, marker_colors=color_scheme),row=1,col=2)

    fig1.update_layout(
        title={
            'text': "Breakdown of total cost by usage category",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    fig2 = px.area(df_by_category_ts, x="hourly_start_time", y="dollars", color="category_name",color_discrete_sequence=color_scheme)
    fig2.update_layout(
    title={
        'text': "Timeseries of cost by usage category",
        'y':0.95,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'})
    return(fig1,fig2)

def cost_by_user_plots():
    df = cqlib.cost_by_user_ts(sdate, edate)
    df_by_user = df.groupby(['user_name']).sum('numeric_only').reset_index()
    df_by_user.loc[len(df_by_user.index)] = ['Total', df_by_user['approximate_credits_used'].sum()]
    df_by_user = df_by_user.round(2)
    df_by_user["percent_usage"] = df_by_user["approximate_credits_used"]/df_by_user[df_by_user["user_name"]=="Total"]["approximate_credits_used"].values[0]*100
    df_by_user["percent_usage"] = df_by_user["percent_usage"].round(3)
    x = df_by_user.loc[df_by_user["percent_usage"]<1.00].sum(axis=0,numeric_only=True)
    df_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].reset_index(drop=True)
    df_by_user_consolidated = df_by_user.loc[df_by_user["percent_usage"] > 1.00].reset_index(drop=True)
    df_by_user_consolidated.loc[len(df_by_user_consolidated)-1.5] = ["Low_usage_users", x["approximate_credits_used"], x["percent_usage"]]
    df_by_user_consolidated= df_by_user_consolidated.sort_index().reset_index(drop=True)
    df_by_user_consolidated.drop(df_by_user_consolidated.tail(1).index,inplace=True)
    fig1 = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "pie"}]],
        subplot_titles=("Credits")
    )

    fig1.add_trace(go.Pie(labels=df_by_user_consolidated['user_name'].tolist(), values=df_by_user_consolidated['approximate_credits_used'].tolist(),name="Credits", rotation=270,marker_colors=color_scheme),row=1,col=1)
    # fig.add_trace(go.Pie(labels=df_by_user['user_name'].tolist(), values=df_by_user['credits'].tolist(),name='Credits', rotation=45,marker_colors=color_scheme),row=1,col=2)

    fig1.update_layout(
        title={
            'text': "Breakdown of total cost by user",
            'y':0.1,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'bottom'})
    df_by_user_ts = df.groupby(['user_name','hourly_start_time']).sum('numeric_only').reset_index()
    df_by_user_ts = df_by_user_ts[~df_by_user_ts.user_name.isin(df_low_usage_users["user_name"].values)]
    df_by_user_ts.reset_index(drop=True)
    fig2 = px.area(df_by_user_ts, x="hourly_start_time", y="approximate_credits_used", color="user_name",color_discrete_sequence=color_scheme)
    
    
    return(df_by_user,df_low_usage_users,df_by_user_consolidated,fig1,fig2)


def cost_by_warehouse_plots():
    df = cqlib.cost_by_wh_ts(sdate, edate)
    df_by_wh = df.groupby(['warehouse_name']).sum('numeric_only').reset_index()
    df_by_wh=df_by_wh.round(2)
    df_by_wh_table = df_by_wh
    df_by_wh_table.loc[len(df.index)] = ['Total', df_by_wh_table['credits'].sum(), df_by_wh_table['dollars'].sum(),  df_by_wh_table['cloud_services_credits'].sum(), df_by_wh_table['cloud_services_dollars'].sum()]
    

    fig1 = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "pie"}]],
    )

    fig1.add_trace(go.Pie(labels=df_by_wh['warehouse_name'].tolist(), values=df_by_wh['dollars'].tolist(),name='dollars',marker_colors=color_scheme),row=1,col=1)

    fig1.update_layout(
        title={
            'text': "Breakdown of total cost by warehouse",
            'y':0.1,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    df_by_wh_ts = df.groupby(['warehouse_name','hourly_start_time']).sum('numeric_only').reset_index()
    fig2 = px.area(df_by_wh_ts, x="hourly_start_time", y="credits", color="warehouse_name",color_discrete_sequence=color_scheme)
    return(df_by_wh_table,fig1,fig2)

def cost_by_pt():
    df=cqlib.cost_by_partner_tool_ts(sdate, edate)
    df_by_pt = df.groupby(['client_application_name']).sum('numeric_only').reset_index()
    df_by_pt_table=df_by_pt
    df_by_pt_table.loc[len(df_by_pt_table.index)] = ['Total', df_by_pt_table['approximate_credits_used'].sum()]
    df_by_pt_table = df_by_pt_table.round(2)

    fig1 = make_subplots(
    rows=1, cols=1,
    specs=[[{"type": "pie"}]],
)

    fig1.add_trace(go.Pie(labels=df_by_pt['client_application_name'].tolist(), values=df_by_pt['approximate_credits_used'].tolist(),name='credits',marker_colors=color_scheme, rotation=45),row=1,col=1)

    fig1.update_layout(
        title={
            'text': "Breakdown of total cost by partner tools",
            'y':0.1,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    df_by_pt_ts = df.groupby(['client_application_name','hourly_start_time']).sum('numeric_only').reset_index()
    fig2 = px.area(df_by_pt_ts, x="hourly_start_time", y="approximate_credits_used", color="client_application_name",color_discrete_sequence=color_scheme)
    df_titles=sorted(df["warehouse_name"].unique())
    df_warehouse = [d for _, d in df.groupby(['warehouse_name'])]
    fig3=[]
    for i in range(len(df_warehouse)):
        fig3.append(px.area(df_warehouse[i], x="hourly_start_time", y="approximate_credits_used", color="client_application_name",color_discrete_sequence=color_scheme,title=df_titles[i]))
    return(df_by_pt_table,fig1,fig2,fig3)

# --------- Query Analysis plots------------------------
def expensive_queries():
    df = qqlib.n_inefficient_queries(sdate,edate,10)
    return(df)











        