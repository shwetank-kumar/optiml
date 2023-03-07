import os
import streamlit as st
import pandas as pd
from copy import deepcopy
from tabulate import tabulate
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from optiml.backend.query_profile import QueryProfile
from PIL import Image

# system import
from optiml.backend.cost_profile import CostProfile, get_previous_dates
from optiml.connection import SnowflakeConnConfig

color_scheme = ["red", "blue", "green", "orange", "purple", "brown", "pink", "gray", "olive", "cyan", "darkviolet",
                "goldenrod", "darkgreen", "chocolate", "lawngreen"]

connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()
cqlib = CostProfile(connection, st.session_state['Schema'])
qqlib = QueryProfile(connection, st.session_state['Schema'])


def format_card_data(df):
    print(df)
    """
    """
    keys = ['Cloud services', 'Compute', 'Storage', 'Total', 'Autoclustering', 'Search Optimization',
            'Materialized views', "Snowpipe", "Replication"]
    card_values = {}

    for key in keys:
        if key in list(df['category_name']):
            card_values[key] = (df[df['category_name'] == key]['dollars'].values[0],
                                df[df['category_name'] == key]['pct_change_dollars'].values[0])
        else:
            card_values[key] = ("--", "0")
    print(card_values)
    return card_values


# Total Usage
def plot_total_usage_df(total_usage_df):
    ## Get data
    # df = cqlib.total_cost_breakdown_ts(sdate, edate)
    df = total_usage_df.fillna('Unassigned')
    ## Get usage for past week
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(),
                                                                 df_by_usage_category['dollars'].sum()]
    df_by_usage_category = df_by_usage_category.round(2)

    ## Get usage for previous week as a predictive sanity check
    p1_sdate, p1_edate = get_previous_dates(st.session_state.sdate, st.session_state.edate, 1)
    df_prev = cqlib.total_cost_breakdown_ts(p1_sdate, p1_edate)
    df_prev = df_prev.fillna('Unassigned')
    df_by_usage_category_prev = df_prev.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category_prev.loc[len(df_by_usage_category_prev.index)] = ['Total',
                                                                           df_by_usage_category_prev['credits'].sum(),
                                                                           df_by_usage_category_prev['dollars'].sum()]
    ## Get percentage change since previous week
    df_by_usage_category_prev = df_by_usage_category_prev.round(2)
    df_by_usage_category.set_index('category_name', inplace=True)
    df_by_usage_category_prev.set_index('category_name', inplace=True)
    df_by_usage_category_prev.rename(columns={"credits": "credits_previous_week", "dollars": "dollars_previous_week"},
                                     inplace=True)

    df_by_usage_category = pd.concat([df_by_usage_category_prev, df_by_usage_category], axis=1)
    df_by_usage_category.reset_index(inplace=True)
    df_by_usage_category["pct_change_dollars"] = round(
        (df_by_usage_category["dollars"] - df_by_usage_category["dollars_previous_week"]) / df_by_usage_category[
            "dollars_previous_week"] * 100, 2)
    usage_df = deepcopy(df_by_usage_category)
    ## Pie charts for total cost breakdown
    df_by_usage_category.reset_index(inplace=True)
    df_by_usage_category.drop(columns=["index"], inplace=True)
    df_by_usage_category = df_by_usage_category.drop(len(df_by_usage_category) - 1)
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "pie"}, {"type": "pie"}]],
        subplot_titles=("Dollars", "Credits")
    )

    fig.add_trace(
        go.Pie(labels=df_by_usage_category['category_name'].tolist(), values=df_by_usage_category['dollars'].tolist(),
               name="Dollars",
               rotation=45, marker_colors=color_scheme), row=1, col=1)
    fig.add_trace(
        go.Pie(labels=df_by_usage_category['category_name'].tolist(), values=df_by_usage_category['credits'].tolist(),
               name='Credits',
               rotation=45, marker_colors=color_scheme), row=1, col=2)

    fig.update_layout(
        title={
            'text': "Total cost by usage category",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'})

    df_by_category_ts = df.groupby(['category_name', 'hourly_start_time']).sum('numeric_only').reset_index()
    tz_fig = px.area(df_by_category_ts, x="hourly_start_time", y="dollars", color="category_name",
                     color_discrete_sequence=color_scheme)
    tz_fig.update_layout(
        title={
            'text': "Timeseries of cost by usage category",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="US Dollars"
    )
    return usage_df, fig, tz_fig


# User Usage
def plot_user_df(user_df):
    ## Get usage for the past week
    df_by_user = user_df.groupby(['user_name']).sum('numeric_only').reset_index()
    df_by_user = df_by_user.round(2)

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_user_ts(st.session_state.p1_sdate, st.session_state.p1_edate)
    df_by_user_prev = df_prev.groupby(['user_name']).sum('numeric_only').reset_index()
    df_by_user_prev = df_by_user_prev.round(2)
    df_by_user_prev.rename(columns={"approximate_credits": "approximate_credits_previous_week"}, inplace=True)

    ## Get percentage change since previous week
    df_by_user.set_index('user_name', inplace=True)
    df_by_user_prev.set_index('user_name', inplace=True)
    df_by_user = pd.concat([df_by_user_prev, df_by_user], axis=1)
    df_by_user.reset_index(inplace=True)
    df_by_user.loc[len(df_by_user.index)] = ['Total', \
                                             df_by_user['approximate_credits_previous_week'].sum(), \
                                             df_by_user['approximate_credits'].sum()]

    df_by_user.fillna({'user_name': 'Unassigned', \
                       'approximate_credits_previous_week': 0, \
                       'approximate_credits': 0
                       }, inplace=True)

    df_by_user["pct_change_credits"] = round((df_by_user["approximate_credits"] \
                                              - df_by_user["approximate_credits_previous_week"]) \
                                             / df_by_user["approximate_credits_previous_week"] * 100, 2)
    df_by_user_copy = deepcopy(df_by_user)
    ## Group low usage users together
    df_by_user = df_by_user[["user_name", "approximate_credits"]]
    df_by_user["percent_usage"] = df_by_user["approximate_credits"] / \
                                  df_by_user[df_by_user["user_name"] == "Total"]["approximate_credits"].values[0] * 100
    df_by_user["percent_usage"] = df_by_user["percent_usage"].round(3)
    idx_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].sum(axis=0, numeric_only=True)
    df_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].reset_index(drop=True)

    ## Drop total
    df_by_user.drop(df_by_user.tail(1).index, inplace=True)

    ## Plot pie
    ## Group low usage users together
    df_by_user = df_by_user.loc[df_by_user["percent_usage"] > 1.00].reset_index(drop=True)
    df_by_user.loc[len(df_by_user) - 1.5] = ["Low_usage_users", \
                                             idx_low_usage_users["approximate_credits"], \
                                             idx_low_usage_users["percent_usage"]]
    df_by_user = df_by_user.sort_index().reset_index(drop=True)

    ## Drop total
    df_by_user.drop(df_by_user.tail(1).index, inplace=True)

    ## Plot pie
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "pie"}]],
        subplot_titles=("Credits")
    )

    fig.add_trace(go.Pie(labels=df_by_user['user_name'].tolist(), \
                         values=df_by_user['approximate_credits'].tolist(), name="Credits", rotation=320,
                         marker_colors=color_scheme), row=1, col=1)

    fig.update_layout(
        title={
            'text': "Total cost by user",
            'y': 0.1,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'bottom'})

    ##Plot timeseries
    df_by_user_ts = user_df.groupby(['user_name', 'hourly_start_time']).sum('numeric_only').reset_index()
    df_by_user_ts = df_by_user_ts[~df_by_user_ts.user_name.isin(df_low_usage_users["user_name"].values)]
    df_by_user_ts.reset_index(drop=True)
    ts_fig = px.area(df_by_user_ts, \
                     x="hourly_start_time", \
                     y="approximate_credits", \
                     color="user_name", \
                     color_discrete_sequence=color_scheme)
    ts_fig.update_layout(
        title={
            'text': "Timeseries of cost by user (except low usage users)",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="Credits used (approx.)"
    )
    return df_by_user_copy, df_low_usage_users, fig, ts_fig


# Warehouse Usage
def plot_warehouse_df(wh_df):
    ## Get usage for the past week
    df_by_wh = wh_df.groupby(['warehouse_name']).sum('numeric_only').reset_index()
    df_by_wh = df_by_wh.round(2)

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_wh_ts(st.session_state.p1_sdate, st.session_state.p1_edate)
    df_by_wh_prev = df_prev.groupby(['warehouse_name']).sum('numeric_only').reset_index()
    df_by_wh_prev = df_by_wh_prev.round(2)
    # df_by_wh_prev.loc[len(df.index)] = ['Total', \
    #                                df_by_wh_prev['credits'].sum(), \
    #                                df_by_wh_prev['cloud_services_credits'].sum()
    #                               ]
    df_by_wh_prev.rename(columns={"credits": "credits_previous_week", \
                                  "cloud_services_credits": "cloud_services_credits_previous_week"}, inplace=True)

    ## Get percentage change since previous week
    df_by_wh.set_index('warehouse_name', inplace=True)
    df_by_wh_prev.set_index('warehouse_name', inplace=True)
    df_by_wh = pd.concat([df_by_wh_prev, df_by_wh], axis=1)
    df_by_wh.reset_index(inplace=True)

    df_by_wh.loc[len(df_by_wh.index)] = ['Total', \
                                         df_by_wh['credits'].sum(), \
                                         df_by_wh['cloud_services_credits'].sum(), \
                                         df_by_wh['credits_previous_week'].sum(), \
                                         df_by_wh['cloud_services_credits_previous_week'].sum(), \
                                         ]

    df_by_wh["pct_change_credits"] = round((df_by_wh["credits"] \
                                            - df_by_wh["credits_previous_week"]) \
                                           / df_by_wh["credits_previous_week"] * 100, 2)
    df_by_wh.fillna({'warehouse_name': 'Unassigned', \
                     'credits_previous_week': 0, \
                     'cloud_services_credits_previous_week': 0, \
                     'credits': 0, \
                     'cloud_service_credits': 0, \
                     'pct_change_credits': 0 \
                     }, inplace=True)

    df_by_wh_print = df_by_wh[["warehouse_name", "credits_previous_week", "credits", "pct_change_credits"]]
    # Remove the last row of totals for the plot
    df_by_wh.drop(df_by_wh.tail(1).index, inplace=True)

    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "pie"}]],
    )

    fig.add_trace(go.Pie(labels=df_by_wh['warehouse_name'].tolist(), \
                         values=df_by_wh['credits'].tolist(), \
                         name='credits', \
                         marker_colors=color_scheme), row=1, col=1)

    fig.update_layout(
        title={
            'text': "Total cost by warehouse",
            'y': 0.1,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'})

    ## Plot timeseries
    df_by_wh_ts = wh_df.groupby(['warehouse_name', 'hourly_start_time']).sum('numeric_only').reset_index()
    ts_fig = px.area(df_by_wh_ts, x="hourly_start_time", y="credits", color="warehouse_name",
                     color_discrete_sequence=color_scheme)
    ts_fig.update_layout(
        title={
            'text': "Timeseries of cost by warehouse",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="Credits used"
    )

    return df_by_wh_print, fig, ts_fig


# Partner Tool Usage
def plot_partner_tool_df(partner_tool_df):
    ## Get usage for the past week
    df = cqlib.cost_by_partner_tool_ts(st.session_state.sdate, st.session_state.edate)
    df_by_pt = df.groupby(['client_application_name']).sum('numeric_only').reset_index()
    df_by_pt = df_by_pt.round(2)
    df_by_pt.loc[len(df.index)] = ['Total', df_by_pt['approximate_credits'].sum()]

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_partner_tool_ts(st.session_state.p1_sdate, st.session_state.p1_edate)
    df_by_pt_prev = df_prev.groupby(['client_application_name']).sum('numeric_only').reset_index()
    df_by_pt_prev = df_by_pt_prev.round(2)
    df_by_pt_prev.loc[len(df_prev.index)] = ['Total', df_by_pt_prev['approximate_credits'].sum()]
    df_by_pt_prev.rename(columns={"approximate_credits": "approximate_credits_previous_week"}, inplace=True)

    ## Get percentage change since previous week
    df_by_pt.set_index('client_application_name', inplace=True)
    df_by_pt_prev.set_index('client_application_name', inplace=True)
    df_by_pt = pd.concat([df_by_pt_prev, df_by_pt], axis=1)
    df_by_pt.reset_index(inplace=True)

    df_by_pt.fillna({'client_application_name': 'Unassigned', \
                     'approximate_credits_previous_week': 0, \
                     'approximate_credits': 0, \
                     'pct_change_credits': 0 \
                     }, inplace=True)

    df_by_pt["pct_change_credits"] = round((df_by_pt["approximate_credits"] \
                                            - df_by_pt["approximate_credits_previous_week"]) \
                                           / df_by_pt["approximate_credits_previous_week"] * 100, 2)
    # Remove the last row of totals for the plot
    df_by_pt.drop(df_by_pt.tail(1).index, inplace=True)

    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "pie"}]],
    )

    fig.add_trace(
        go.Pie(labels=df_by_pt['client_application_name'].tolist(), values=df_by_pt['approximate_credits'].tolist(),
               name='credits', marker_colors=color_scheme, rotation=45), row=1, col=1)

    fig.update_layout(
        title={
            'text': "Breakdown of total cost by partner tools",
            'y': 0.1,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'})

    df_by_pt_ts = partner_tool_df.groupby(['client_application_name', 'hourly_start_time']).sum(
        'numeric_only').reset_index()
    ts_fig = px.area(df_by_pt_ts, x="hourly_start_time", y="approximate_credits", color="client_application_name",
                     color_discrete_sequence=color_scheme)
    ts_fig.update_layout(
        title={
            'text': "Timeseries of cost by partner tools",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="Credits used (approx.)"
    )

    return df_by_pt, fig, ts_fig


import textwrap as tw


def generate_resource_monitor_sql(resource_monitor_name="monitor_1", \
                                  credit_quota=10, \
                                  periodicity="weekly", \
                                  start_timestamp_ltz="2023-02-18 00:00:00 PST", \
                                  percentage_of_monitor=100, \
                                  action="notify", \
                                  warehouse_name="warehouse_1"):
    resource_monitor_sql = tw.dedent(f"""
                            USE ROLE ACCOUNTADMIN;
                            CREATE OR REPLACE RESOURCE MONITOR {resource_monitor_name} 
                            WITH CREDIT_QUOTA={credit_quota}
                            FREQUENCY={periodicity}
                            START_TIMESTAMP={start_timestamp_ltz}
                            TRIGGERS ON {percentage_of_monitor} PERCENT DO {action};
                            ALTER WAREHOUSE {warehouse_name} SET RESOURCE_MONITOR={resource_monitor_name};
                            """)
    return resource_monitor_sql


def total_query_fails(df):
    df_by_day = df.groupby(['day']).agg(
        {'n_success': 'sum', 'n_fail': 'sum', 'credits_success': 'sum', 'credits_fail': 'sum'}).reset_index()
    trace1 = go.Bar(
        x=df_by_day['day'],
        y=df_by_day['n_fail'],
        name="Execution fail count",
    )

    trace2 = go.Scatter(
        mode='lines+markers',
        x=df_by_day['day'],
        y=df_by_day['credits_fail'],
        name="Credits",
        yaxis='y2',
    )

    data = [trace1, trace2]

    layout = go.Layout(
        title_text='Query fails and credits per day',
        yaxis=dict(
            title="Count number",
            showgrid=False,
        ),
        yaxis2=dict(
            title="Credits",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        xaxis=dict(
            title="Date (UTC)"
        ),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.99
        ),
        barmode="stack"
    )
    fig = go.Figure(data=data, layout=layout)
    metrics = {}
    metrics["total_success"] = total_success = sum(df['n_success'])
    metrics["total_fail"] = total_fail = sum(df['n_fail'])
    metrics["pct_fail"] = round(total_fail / (total_fail + total_success) * 100, 2)
    metrics["credits_success"] = credits_success = round(sum(df['credits_success']), 2)
    metrics["credits_fail"] = credits_fail = sum(df['credits_fail'])
    metrics["pct_credits_fail"] = round(credits_fail / (credits_fail + credits_success) * 100, 2)
    return df_by_day, fig, metrics


def warehouse_by_day(df):
    df_by_wh = df.groupby(["warehouse_name", "day"]).agg(
        {'n_success': 'sum', 'n_fail': 'sum', 'credits_success': 'sum', 'credits_fail': 'sum'}).reset_index()
    failed_by_wh_fig = px.bar(df_by_wh, x="day", y="n_fail", color="warehouse_name", title="Number failed by warehouse")
    credit_failed_fig = px.bar(df_by_wh, x="day", y="credits_fail", color="warehouse_name",
                               title="Credits failed by warehouse")
    return failed_by_wh_fig, credit_failed_fig


def get_resource_monitor_values(ts, te):
    print("Ts te: ", ts, te)
    df_compute = cqlib.cost_of_compute_ts(ts, te)
    df_compute.drop(df_compute.loc[df_compute['warehouse_name'] == "CLOUD_SERVICES_ONLY"].index, inplace=True)
    df_by_wh_day = df_compute.groupby([
        pd.Grouper(key='hourly_start_time', axis=0, freq='D', sort=True),
        pd.Grouper('warehouse_name')
    ]).sum()
    df_by_wh_day.reset_index(inplace=True)
    df_by_wh_day.rename(columns={"hourly_start_time": "day"}, errors="raise", inplace=True)
    df_stats_by_wh_day = df_by_wh_day.groupby("warehouse_name")["credits"].agg(['mean', 'std'])
    df_stats_by_wh_day.reset_index(inplace=True)
    df_stats_by_wh_day["credits_three_sigma_plus"] = df_stats_by_wh_day["mean"] + 3 * df_stats_by_wh_day["std"]
    df_stats_by_wh_day["credits_three_sigma_minus"] = df_stats_by_wh_day["mean"] - 3 * df_stats_by_wh_day["std"]
    df_stats_by_wh_day["credits_three_sigma_minus"] = df_stats_by_wh_day["credits_three_sigma_minus"].clip(0, None)
    df_stats_by_wh_day = df_stats_by_wh_day.round(2)
    df_stats_by_wh_day.drop(columns=["mean", "std"], inplace=True)
    df_stats_by_wh_day.reset_index(inplace=True, drop=True)
    return df_by_wh_day, df_stats_by_wh_day


def resource_moniter_queries():
    df_by_wh, resource_monitor = get_resource_monitor_values(st.session_state["training_start"],
                                                             st.session_state['training_end'])
    resource_monitor_queries = []
    for idx, wh_name in enumerate(resource_monitor["warehouse_name"].unique()):
        resource_monitor_name = resource_monitor.loc[idx]["warehouse_name"] + '_RESOURCE_MONITOR'
        credit_quota = resource_monitor.loc[idx]["credits_three_sigma_plus"]
        start_timestamp_ltz = "YYYY-MM-DD HH:MM:SS PST"
        periodicity = "DAILY"
        percentage_of_monitor = 100
        action = "NOTIFY"
        warehouse_name = resource_monitor.loc[idx]["warehouse_name"]
        resource_monitor_queries.append(generate_resource_monitor_sql(resource_monitor_name, \
                                                                      credit_quota, \
                                                                      periodicity, \
                                                                      start_timestamp_ltz, \
                                                                      percentage_of_monitor, \
                                                                      action, \
                                                                      warehouse_name))
    return resource_monitor_queries


def show_dashboard(**kwargs):
    print("Under Dashboard")
    total_cost_df = kwargs.get('total_cost_df')
    cost_by_user_df = kwargs.get('cost_by_user_df')
    cost_by_wh_df = kwargs.get('cost_by_wh_df')
    cost_by_partner_tools_df = kwargs.get('cost_by_partner_tools_df')
    st.header("Snowflake Resource Dashboard 🎆")
    st.warning("👉 Total Resource usage")
    df_by_usage_category, fig, ts_fig = plot_total_usage_df(total_cost_df)
    total_usage = df_by_usage_category.round({"credits_previous_week": 2, "dollars_previous_week": 2, })

    row1_cols = st.columns([1, 1])
    # row1_cols[0].write('Credit and dollar usage by category (Previous Week)')
    # row1_cols[0].dataframe(total_usage[["category_name", "credits_previous_week", "dollars_previous_week"]],
    #                        use_container_width=True)
    # row1_cols[1].write('Credit and dollar usage by category (Current Week)')
    # row1_cols[1].dataframe(total_usage[['category_name', 'credits', 'dollars']], use_container_width=True)
    metric_cols = st.columns([2, 3])
    metric_cols[0].success(""" Comparison of metric from previous month.""")
    metric1, metric2, metric3, metric4, metric5 = st.columns(5)
    card_values = format_card_data(total_usage)
    metric1.metric("Autoclustering", f"{card_values['Autoclustering'][0]}", f"{card_values['Autoclustering'][1]}")
    metric1.metric("Search Optimization", f"{card_values['Search Optimization'][0]}",
                   f"{card_values['Search Optimization'][1]}")
    metric2.metric("Cloud services", f"{card_values['Cloud services'][0]}", f"{card_values['Cloud services'][1]}")
    metric2.metric("Materialized views", f"{card_values['Materialized views'][0]}",
                   f"{card_values['Materialized views'][1]}")
    metric3.metric("Compute", f"{card_values['Compute'][0]}", f"{card_values['Compute'][1]}")
    metric3.metric("Snowpipe", f"{card_values['Snowpipe'][0]}", f"{card_values['Snowpipe'][1]}")
    metric4.metric("Storage", f"{card_values['Storage'][0]}", f"{card_values['Storage'][1]}")
    metric4.metric("Replication", f"{card_values['Replication'][0]}", f"{card_values['Replication'][1]}")
    metric5.metric("Total", f"{card_values['Total'][0]}", f"{card_values['Total'][1]}")
    total_cost_cols = st.columns([2, 3])
    total_cost_cols[0].success("Pie Chart representing the total credit and dollar usage")
    st.plotly_chart(fig, use_container_width=True)
    total_cost_ts = st.columns([2, 3])
    total_cost_ts[0].success("Total Cost in timeseries.")
    st.plotly_chart(ts_fig, use_container_width=True)
    st.warning("👉 Resource usage by :: User.")
    st.write("")
    # st.subheader("Credit and dollar usage by user with low usage users consolidated (Current month)")
    df_by_user, df_low_usage_users, user_fig, ts_fig = plot_user_df(cost_by_user_df)
    st.success('Users: Credit consumption trends')
    st.dataframe(df_by_user, use_container_width=True)
    user_cols = st.columns([1, 1.5])
    user_cols[0].success("List of low usage users (<1% of credits) with usage (Current month)")
    user_cols[0].dataframe(df_low_usage_users, use_container_width=True)
    user_cols[1].success("Plot for total cost by user")
    user_cols[1].plotly_chart(user_fig, use_container_width=True)
    st.subheader("Timeseries plot for cost by user")
    st.plotly_chart(ts_fig, use_container_width=True)
    st.warning("👉 Resource usage by :: Warehouse.")
    df_by_wh_print, fig, ts_fig = plot_warehouse_df(cost_by_wh_df)
    wh_cols = st.columns([1.5, 1])
    wh_cols[0].write("")
    wh_cols[0].success("Total cost by warehouse")
    wh_cols[0].write("")
    wh_cols[0].write("")
    wh_cols[0].dataframe(df_by_wh_print, use_container_width=True)
    wh_cols[1].plotly_chart(fig, use_container_width=True)
    st.success("Timeseries plot for cost by Warehouse")
    st.plotly_chart(ts_fig, use_container_width=True)
    st.warning("👉 Resource usage ::  Partner Tools.")
    st.write("")
    df_by_pt, fig, ts_fig = plot_partner_tool_df(cost_by_partner_tools_df)
    partner_cols = st.columns([1.5, 1])
    partner_cols[0].write("")
    partner_cols[0].success("Cost by Partner Tools")
    partner_cols[0].write("")
    partner_cols[0].write("")
    partner_cols[0].dataframe(df_by_pt, use_container_width=True)
    partner_cols[1].plotly_chart(fig, use_container_width=True)
    st.success("Timeseries plot for cost by Partner Tools")
    st.plotly_chart(ts_fig, use_container_width=True)
    resource_monitor_queries = resource_moniter_queries()
    print(resource_monitor_queries)
    st.warning("👉 Recommended Resource Monitor for Warehouses")
    for query in resource_monitor_queries:
        with st.expander(f"🎫 {query.split('=')[-1]}".strip(";")):
            st.write(query)


def query_dashboard(**kwargs):
    st.header("Snowflake Query Profile Dashboard 🎆")
    df_by_day, fig, metrics = total_query_fails(kwargs['query_execution_status'])
    total_data_cols = st.columns([1, 1])
    total_data_cols[0].write("")
    total_data_cols[0].success("Dataframe Showing Query Profiling")
    total_data_cols[0].dataframe(df_by_day, use_container_width=True)
    total_data_cols[1].plotly_chart(fig, use_container_width=True)
    st.success("Summary Stats :: Credits and counts")
    total_metric_cols = st.columns(3)
    total_metric_cols[0].metric("Total Success", f"{metrics['total_success']}")
    total_metric_cols[0].metric("Total Fail", f"{metrics['total_fail']}")
    total_metric_cols[1].metric("Percentage Fail", f"{metrics['pct_fail']}%")
    total_metric_cols[1].metric("Credits Success", f"{metrics['credits_success']}")
    total_metric_cols[2].metric("Credits Fail", f"{metrics['credits_fail']}")
    total_metric_cols[2].metric("percent credits_fail", f"{metrics['pct_credits_fail']}%")
    failed_by_wh_fig, credit_failed_fig = warehouse_by_day(kwargs['query_execution_status'])
    st.warning("👉 By warehouse by day.")
    warehouse_cols = st.columns([1, 1])
    warehouse_cols[0].plotly_chart(failed_by_wh_fig, use_container_width=True)
    warehouse_cols[1].plotly_chart(credit_failed_fig, use_container_width=True)

    df_expensive_queries_failed = qqlib.queries_by_execution_status(st.session_state.sdate, st.session_state.edate,
                                                                    'FAIL')
    df_unique_fail = qqlib.get_unique_failed_queries_with_metrics_ordered(df_expensive_queries_failed, 'credits')
    df_unique_fail.reset_index(inplace=True)
