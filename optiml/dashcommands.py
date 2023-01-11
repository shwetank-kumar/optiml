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
from queries import SNFLKQuery
qlib = SNFLKQuery(connection, 'KIV', cache_dir)
sdate = '2022-09-12'
edate = '2022-10-12'
print(f"The analysis is carried our for date range {sdate} to {edate}")

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
    df = qlib.total_cost_breakdown_ts(p1_sdate, p1_edate)
    df = df.fillna('Unassigned')
    df=df.groupby("category_name").sum("numeric_only").reset_index()
    df.loc[len(df.index)] = ['Total', df['credits'].sum(), df['dollars'].sum()]
    df_prev=df.T.reset_index(drop=True)
    seq=["Category","Previous month credits", "Previous month dollars"]
    df_prev.insert(0,"Category",seq)
    df_prev.columns = df_prev.iloc[0]
    df_prev= df_prev[1:]
    return(df,df_prev)


def current_month_cost():
    df=qlib.total_cost_breakdown_ts(sdate,edate)
    df = df.fillna('Unassigned')
    df=df.groupby("category_name").sum("numeric_only").reset_index()
    df.loc[len(df.index)] = ['Total', df['credits'].sum(), df['dollars'].sum()]
    df_current=df.T.reset_index(drop=True)
    seq=["Category","Previous month credits", "Previous month dollars"]
    df_current.insert(0,"Category",seq)
    df_current.columns = df_current.iloc[0]
    df_current= df_current[1:]
    return(df,df_current)

def percentage_change():
    df_prev1,df_prev2=previous_month_cost()
    df_current1,df_current2=current_month_cost()
    df_change = pd.DataFrame().assign(category_name=df_prev1["category_name"])
    df_change["percent_change"] = ((df_current1["dollars"] - df_prev1["dollars"])/df_prev1["dollars"]*100).round(2)
    df_change=df_change.T.reset_index(drop=True)
    seq=["Category","Percentage change"]
    df_change.insert(0,"Category",seq)
    df_change.columns = df_change.iloc[0]
    df_change= df_change[1:]
    return(df_change)

def usage_cost():
    df_prev1,df_prev2=previous_month_cost()
    df_current1,df_current2=current_month_cost()
    df_change=percentage_change()
    all_df=[df_prev2,df_current2,df_change]
    df_usage_all=pd.concat(all_df).reset_index(drop=True)
    return(df_usage_all)

# def cost_by_usage():
#     df=qlib.total_cost_breakdown_ts(sdate,edate)
#     df_by_category_ts = df.groupby(['category_name','hourly_start_time']).sum('numeric_only').reset_index()
#     df_compute = df_by_category_ts[df_by_category_ts["category_name"] == "Compute"].round(2)
#     avg_consumption = df_compute.mean().round(2)
#     max_consumption = df_compute.loc[df_compute['credits'].idxmax()]
#     max_consumption.drop("category_name", inplace=True)
#     min_consumption = df_compute.loc[df_compute['credits'].idxmin()]
#     min_consumption.drop("category_name", inplace=True)
#     avg_consumption=avg_consumption.to_frame()
#     max_consumption=max_consumption.to_frame()
#     min_consumption=min_consumption.to_frame()
#     return(avg_consumption,max_consumption,min_consumption)

def usage_category():
    df=qlib.total_cost_breakdown_ts(sdate,edate)
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(), df_by_usage_category['dollars'].sum()]
    df_by_usage_category = df_by_usage_category.round(2)
    df_by_usage_category.reset_index(inplace=True)
    df_by_usage_category.drop(columns=["index"], inplace=True)
    df_by_usage_category = df_by_usage_category.drop(len(df_by_usage_category)-1) 
    df_by_category_ts = df.groupby(['category_name','hourly_start_time']).sum('numeric_only').reset_index()
    return(df_by_usage_category,df_by_category_ts)

def cost_analysis_plots():
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

