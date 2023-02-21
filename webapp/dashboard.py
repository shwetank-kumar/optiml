import os
import streamlit as st
import pandas as pd

from tabulate import tabulate
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from PIL import Image

# system import
from optiml.backend.cost_profile import CostProfile, get_previous_dates
from optiml.connection import SnowflakeConnConfig

color_scheme = ["red", "blue", "green", "orange", "purple", "brown", "pink", "gray", "olive", "cyan", "darkviolet",
                "goldenrod",
                "darkgreen", "chocolate", "lawngreen"]

connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()

# schema = "KIV.ACCOUNT_USAGE
Schema = "KIV.ACCOUNT_USAGE"
cqlib = CostProfile(connection, Schema)

sdate = '2022-10-11'
edate = '2022-10-21'
p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)


# @st.cache
# def total_cost_curr_month(total_cost_df):
#     # df = cqlib.total_cost_breakdown_ts(sdate, edate)
#     df = total_cost_df.fillna('Unassigned')
#     df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
#     df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(),
#                                                                  df_by_usage_category['dollars'].sum()]
#     df_by_usage_category = df_by_usage_category.round(2)
#     print('Credit and dollar usage by category (Current month)')
#     print('---------------------------------------------------')
#     tabular_df = tabulate(df_by_usage_category, headers='keys', tablefmt='rounded_outline', showindex=False)
#     # st.dataframe(tabular_df)
#     return df_by_usage_category


# @st.cache
# def total_cost_prev_month():
#     p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)
#     df_prev = cqlib.total_cost_breakdown_ts(p1_sdate, p1_edate)
#     df_prev = df_prev.fillna('Unassigned')
#     df_by_usage_category_prev = df_prev.groupby("category_name").sum("numeric_only").reset_index()
#     df_by_usage_category_prev.loc[len(df_by_usage_category_prev.index)] = ['Total',
#                                                                            df_by_usage_category_prev['credits'].sum(),
#                                                                            df_by_usage_category_prev['dollars'].sum()]
#     df_by_usage_category_prev = df_by_usage_category_prev.round(2)
#     return df_by_usage_category_prev


def df_cost_by_usage(sdate, edate):
    ## Get data
    df = cqlib.total_cost_breakdown_ts(sdate, edate)
    df = df.fillna('Unassigned')
    ## Get usage for past week
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(),
                                                                 df_by_usage_category['dollars'].sum()]
    df_by_usage_category = df_by_usage_category.round(2)

    ## Get usage for previous week as a predictive sanity check
    p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)
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
    return df_by_usage_category


def format_card_data(df):
    print(df)
    """
     'Cloud services',
     'Compute',
     'Storage',
     'Total'
    """
    keys = ['Cloud services', 'Compute', 'Storage', 'Total', 'Auto-Clustering', 'Search Optimization',
            'Materialized View', "Snowpipe", "Replication"]
    card_values = {}

    for key in keys:
        if key in list(df['category_name']):
            card_values[key] = (df[df['category_name'] == key]['dollars'].values[0],
                                df[df['category_name'] == key]['pct_change_dollars'].values[0])
        else:
            card_values[key] = ("--", "0")
    print(card_values)
    return card_values


def cost_by_user(total_cost_df):
    ## Get usage for the past week
    df = cqlib.cost_by_user_ts(sdate, edate)
    df_by_user = df.groupby(['user_name']).sum('numeric_only').reset_index()
    df_by_user = df_by_user.round(2)

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_user_ts(p1_sdate, p1_edate)
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

    df_by_user = df_by_user[["user_name", "approximate_credits"]]
    df_by_user["percent_usage"] = df_by_user["approximate_credits"] / \
                                  df_by_user[df_by_user["user_name"] == "Total"]["approximate_credits"].values[0] * 100
    df_by_user["percent_usage"] = df_by_user["percent_usage"].round(3)
    idx_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].sum(axis=0, numeric_only=True)
    df_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].reset_index(drop=True)

    return df_by_user, df_low_usage_users


def df_cost_by_wh(wh_df):
    ## Get usage for the past week
    df = cqlib.cost_by_wh_ts(sdate, edate)
    df_by_wh = df.groupby(['warehouse_name']).sum('numeric_only').reset_index()
    df_by_wh = df_by_wh.round(2)

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_wh_ts(p1_sdate, p1_edate)
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
    return df_by_wh, df_by_wh_print


def plot_cost_by_wh(df_by_wh):
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
    fig.show()
    # print('Warehouses: Credit consumption trends')
    # print('-------------------------------------')
    # print(tabulate(df_by_wh_print, headers='keys', tablefmt='rounded_outline', showindex=False))


def plot_user_df1(user_df):
    ## Get usage for the past week
    df_by_user = user_df.groupby(['user_name']).sum('numeric_only').reset_index()
    df_by_user = df_by_user.round(2)

    ## Get usage for previous week as a predictive sanity check
    p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)
    df_prev = cqlib.cost_by_user_ts(p1_sdate, p1_edate)
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
    df_by_user = df_by_user[["user_name", "approximate_credits"]]
    df_by_user["percent_usage"] = df_by_user["approximate_credits"] / \
                                  df_by_user[df_by_user["user_name"] == "Total"]["approximate_credits"].values[0] * 100
    df_by_user["percent_usage"] = df_by_user["percent_usage"].round(3)
    idx_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].sum(axis=0, numeric_only=True)
    df_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].reset_index(drop=True)

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
                         values=df_by_user['approximate_credits'].tolist(), \
                         name="Credits", rotation=320, \
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
    print("Hello")
    return df_by_user, df_low_usage_users, fig, ts_fig


def plot_warehouse_df(wh_df):
    ## Get usage for the past week
    df_by_wh = wh_df.groupby(['warehouse_name']).sum('numeric_only').reset_index()
    df_by_wh = df_by_wh.round(2)

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_wh_ts(p1_sdate, p1_edate)
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


def plot_partner_df(partner_tool_df):
    ## Get usage for the past week
    df = cqlib.cost_by_partner_tool_ts(sdate, edate)
    df_by_pt = df.groupby(['client_application_name']).sum('numeric_only').reset_index()
    df_by_pt = df_by_pt.round(2)
    df_by_pt.loc[len(df.index)] = ['Total', df_by_pt['approximate_credits'].sum()]

    ## Get usage for previous week as a predictive sanity check
    df_prev = cqlib.cost_by_partner_tool_ts(p1_sdate, p1_edate)
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


def plot_cost_by_warehouse(cost_by_wh):
    # df_by_wh = cost_by_wh.groupby(['warehouse_name']).sum('numeric_only').reset_index()
    # df_by_wh = df_by_wh.round(2)
    # df_by_wh.loc[len(cost_by_wh.index)] = ['Total', df_by_wh['credits'].sum(), df_by_wh['dollars'].sum(),
    #                                        df_by_wh['cloud_services_credits'].sum(),
    #                                        df_by_wh['cloud_services_dollars'].sum()]
    # print('Credit and dollar usage overall and for cloud services by warehouse (Current month)')
    df_by_wh_ts = cost_by_wh.groupby(['warehouse_name', 'hourly_start_time']).sum('numeric_only').reset_index()
    ##TODO: Investigate why tunring off cloud services only makes daily refresh plot jump in some points
    fig = px.area(df_by_wh_ts, x="hourly_start_time", y="credits", color="warehouse_name",
                  color_discrete_sequence=color_scheme)
    fig.update_layout(
        title={
            'text': "Timeseries of cost by warehouse",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="Credits used"
    )
    # fig.show()
    return fig


def plot_total_usage_ts(total_cost_df):
    df_by_category_ts = total_cost_df.groupby(['category_name', 'hourly_start_time']).sum('numeric_only').reset_index()
    fig = px.area(df_by_category_ts, x="hourly_start_time", y="dollars", color="category_name",
                  color_discrete_sequence=color_scheme)
    fig.update_layout(
        title={
            'text': "Timeseries of cost by usage category",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="US Dollars"
    )
    # fig.show()
    return fig


def plot_total_usage(total_cost_df):
    ## Get data
    df = cqlib.total_cost_breakdown_ts(sdate, edate)
    df = df.fillna('Unassigned')
    ## Get usage for past week
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(),
                                                                 df_by_usage_category['dollars'].sum()]
    df_by_usage_category = df_by_usage_category.round(2)

    ## Get usage for previous week as a predictive sanity check
    p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)
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
    return fig


def plot_df_by_user(df_by_user):
    df_by_user.drop(df_by_user.tail(1).index, inplace=True)
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"type": "pie"}]],
        subplot_titles=("Credits"),
    )

    fig.add_trace(
        go.Pie(labels=df_by_user['user_name'].tolist(), values=df_by_user['approximate_credits_used'].tolist(),
               name="Credits", rotation=270, marker_colors=color_scheme), row=1, col=1)

    # fig.add_trace(go.Pie(labels=df_by_user['user_name'].tolist(), values=df_by_user['credits'].tolist(),name='Credits'
    # , rotation=45,marker_colors=color_scheme),row=1,col=2)

    fig.update_layout(
        title={
            'text': "Breakdown of total cost by user",
            'y': 0.1,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'bottom'})
    # fig.show()
    return fig


def plot_cost_by_user_ts(df):
    df_by_user = df.groupby(['user_name']).sum('numeric_only').reset_index()
    df_by_user = df_by_user.round(2)
    df_by_user.loc[len(df_by_user.index)] = ['Total', df_by_user['approximate_credits_used'].sum()]
    df_by_user["percent_usage"] = df_by_user["approximate_credits_used"] / \
                                  df_by_user[df_by_user["user_name"] == "Total"]["approximate_credits_used"].values[
                                      0] * 100
    df_by_user["percent_usage"] = df_by_user["percent_usage"].round(3)
    x = df_by_user.loc[df_by_user["percent_usage"] < 1.00].sum(axis=0, numeric_only=True)
    df_low_usage_users = df_by_user.loc[df_by_user["percent_usage"] < 1.00].reset_index(drop=True)
    df_by_user = df_by_user.loc[df_by_user["percent_usage"] > 1.00].reset_index(drop=True)
    df_by_user.loc[len(df_by_user) - 1.5] = ["Low_usage_users", x["approximate_credits_used"], x["percent_usage"]]
    df_by_user = df_by_user.sort_index().reset_index(drop=True)
    df_by_user_ts = df.groupby(['user_name', 'hourly_start_time']).sum('numeric_only').reset_index()
    df_by_user_ts = df_by_user_ts[~df_by_user_ts.user_name.isin(df_low_usage_users["user_name"].values)]
    df_by_user_ts.reset_index(drop=True)
    fig = px.area(df_by_user_ts, x="hourly_start_time", y="approximate_credits_used", color="user_name",
                  color_discrete_sequence=color_scheme)
    fig.update_layout(
        title={
            'text': "Timeseries of cost by user",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="Credits used (approx.)"
    )
    return fig


def plot_cost_by_partner_tools(cost_by_partner_tools_df):
    df_by_pt_ts = cost_by_partner_tools_df.groupby(['client_application_name', 'hourly_start_time']).sum(
        'numeric_only').reset_index()
    fig = px.area(df_by_pt_ts, x="hourly_start_time", y="approximate_credits_used", color="client_application_name",
                  color_discrete_sequence=color_scheme)
    fig.update_layout(
        title={
            'text': "Timeseries of cost by partner tools",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Hourly start time (UTC)",
        yaxis_title="Credits used (approx.)"
    )
    return fig


def show_dashboard(**kwargs):
    print("Under Dashboard")
    total_cost_df = kwargs.get('total_cost_df')
    cost_by_user_df = kwargs.get('cost_by_user_df')
    cost_by_wh_df = kwargs.get('cost_by_wh_df')
    cost_by_partner_tools_df = kwargs.get('cost_by_partner_tools_df')

    st.header("Snowflake Resource Dashboard 🎈")
    st.warning("Total Resource usage")

    # prev_cost_df = total_cost_prev_month()
    # curr_cost_df = total_cost_curr_month(total_cost_df)
    row1_cols = st.columns([1, 1])
    row1_cols[0].write('Credit and dollar usage by category (Previous Week)')
    total_usage = df_cost_by_usage(sdate, edate)
    total_usage = total_usage.round({"credits_previous_week": 2, "dollars_previous_week": 2, })
    total_usage[['credits_previous_week', 'dollars_previous_week']] = total_usage[
        ['credits_previous_week', 'dollars_previous_week']].apply(lambda x: round(x, 2),result_type='expand')
    total_usage.round(decimals={'credits_previous_week': 2})
    row1_cols[0].dataframe(total_usage[["category_name", "credits_previous_week", "dollars_previous_week"]],
                           use_container_width=True)
    row1_cols[1].write('Credit and dollar usage by category (Current Week)')
    row1_cols[1].dataframe(total_usage[['category_name', 'credits', 'dollars']], use_container_width=True)
    st.success(""" Comparison of metric from previous month.""")
    metric1, metric2, metric3, metric4, metric5 = st.columns(5)
    card_values = format_card_data(total_usage)
    metric1.metric("Auto-Clustering", f"{card_values['Auto-Clustering'][0]}", f"{card_values['Auto-Clustering'][1]}")
    metric1.metric("Search Optimization", f"{card_values['Search Optimization'][0]}",
                   f"{card_values['Search Optimization'][1]}")
    metric2.metric("Cloud services", f"{card_values['Cloud services'][0]}", f"{card_values['Cloud services'][1]}")
    metric2.metric("Materialized View", f"{card_values['Materialized View'][0]}",
                   f"{card_values['Materialized View'][1]}")
    metric3.metric("Compute", f"{card_values['Compute'][0]}", f"{card_values['Compute'][1]}")
    metric3.metric("Snowpipe", f"{card_values['Snowpipe'][0]}", f"{card_values['Snowpipe'][1]}")
    metric4.metric("Storage", f"{card_values['Storage'][0]}", f"{card_values['Storage'][1]}")
    metric4.metric("Replication", f"{card_values['Replication'][0]}", f"{card_values['Replication'][1]}")
    metric5.metric("Total", f"{card_values['Total'][0]}", f"{card_values['Total'][1]}")
    st.plotly_chart(plot_total_usage(total_cost_df), use_container_width=True)
    st.success("Total Cost in timeseries.")
    st.plotly_chart(plot_total_usage_ts(total_cost_df), use_container_width=True)
    st.warning("Resource usage by :: User.")
    st.subheader("Credit and dollar usage by user with low usage users consolidated (Current month)")
    df_by_user, df_low_usage_users, user_fig, ts_fig = plot_user_df1(cost_by_user_df)
    user_cols = st.columns([1, 1])
    user_cols[0].dataframe(df_by_user, use_container_width=True)
    user_cols[1].dataframe(df_low_usage_users, use_container_width=True)
    st.subheader("Total cost by user")
    st.plotly_chart(user_fig, use_container_width=True)
    st.subheader("Timeseries plot for cost by user")
    st.plotly_chart(ts_fig, use_container_width=True)

    st.warning("Resource usage by :: Warehouse.")
    df_by_wh_print, fig, ts_fig = plot_warehouse_df(cost_by_wh_df)
    wh_cols = st.columns([1, 1])

    wh_cols[0].write("")
    wh_cols[0].write("")
    wh_cols[0].write("")
    wh_cols[0].subheader("Total cost by warehouse")
    wh_cols[0].dataframe(df_by_wh_print, use_container_width=True)
    wh_cols[1].plotly_chart(fig, use_container_width=True)
    st.subheader("Timeseries plot for cost by Warehouse")
    st.plotly_chart(ts_fig, use_container_width=True)
    # st.plotly_chart(plot_cost_by_warehouse(cost_by_wh_df), use_container_width=True)
    st.warning("Resource usage ::  Partner Tools.")
    st.write("")
    df_by_pt, fig, ts_fig = plot_partner_df(cost_by_partner_tools_df)
    partner_cols = st.columns([1.5, 1])
    partner_cols[0].write("")
    partner_cols[0].subheader("Cost by Partner Tools")
    partner_cols[0].write("")
    partner_cols[0].write("")
    partner_cols[0].dataframe(df_by_pt, use_container_width=True)
    partner_cols[1].plotly_chart(fig, use_container_width=True)
    st.subheader("Timeseries plot for cost by Partner Tools")
    st.plotly_chart(ts_fig, use_container_width=True)
    # st.plotly_chart(plot_cost_by_partner_tools(cost_by_partner_tools_df), use_container_width=True)
