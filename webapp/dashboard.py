import os
import streamlit as st
from tabulate import tabulate
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from PIL import Image

# system import
from backend.cost_profile import CostProfile, get_previous_dates
from connection import SnowflakeConnConfig

color_scheme = ["red", "blue", "green", "orange", "purple", "brown", "pink", "gray", "olive", "cyan", "darkviolet",
                "goldenrod",
                "darkgreen", "chocolate", "lawngreen"]


connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()
cache_dir = os.path.expanduser('~/data/kiva')
# Initialize query library
cqlib = CostProfile(connection, 'KIV', cache_dir)

sdate = '2022-10-11'
edate = '2022-10-21'

if 'raw_df' in st.session_state:
    st.session_state['raw_df'] = df = cqlib.total_cost_breakdown_ts(sdate, edate)


@st.cache
def total_cost_curr_month(df):
    # df = cqlib.total_cost_breakdown_ts(sdate, edate)
    df = df.fillna('Unassigned')
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category.loc[len(df_by_usage_category.index)] = ['Total', df_by_usage_category['credits'].sum(),
                                                                 df_by_usage_category['dollars'].sum()]
    df_by_usage_category = df_by_usage_category.round(2)
    print('Credit and dollar usage by category (Current month)')
    print('---------------------------------------------------')
    tabular_df = tabulate(df_by_usage_category, headers='keys', tablefmt='rounded_outline', showindex=False)
    # st.dataframe(tabular_df)
    return df_by_usage_category


@st.cache
def total_cost_prev_month():
    p1_sdate, p1_edate = get_previous_dates(sdate, edate, 1)
    df_prev = cqlib.total_cost_breakdown_ts(p1_sdate, p1_edate)
    df_prev = df_prev.fillna('Unassigned')
    df_by_usage_category_prev = df_prev.groupby("category_name").sum("numeric_only").reset_index()
    df_by_usage_category_prev.loc[len(df_by_usage_category_prev.index)] = ['Total',
                                                                           df_by_usage_category_prev['credits'].sum(),
                                                                           df_by_usage_category_prev['dollars'].sum()]
    df_by_usage_category_prev = df_by_usage_category_prev.round(2)
    return df_by_usage_category_prev


def plot_total_usage_ts(df):
    df_by_category_ts = df.groupby(['category_name', 'hourly_start_time']).sum('numeric_only').reset_index()
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


def plot_total_usage(df):
    df_by_usage_category = df.groupby("category_name").sum("numeric_only").reset_index()
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
               name="Dollars", hole=0.3,
               rotation=45, marker_colors=color_scheme), row=1, col=1)
    fig.add_trace(
        go.Pie(labels=df_by_usage_category['category_name'].tolist(), values=df_by_usage_category['credits'].tolist(),
               name='Credits', hole=0.3,
               rotation=45, marker_colors=color_scheme), row=1, col=2)

    fig.update_layout(
        title={
            'text': "Breakdown of total cost by usage category",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    # fig.show()
    # st.plotly_chart(fig)
    return fig


def show_dashboard(df):
    st.header("Snowflake Resource Dashboard 🎈")
    st.write("")
    st.write("")
    st.write("")
    # st.write("""Breakdown of cost based on the usage category.
    #                         This helps you track the consumption and cost.""")
    prev_cost_df = total_cost_prev_month()
    curr_cost_df = total_cost_curr_month(df)
    row1_cols = st.columns([1, 1])
    row1_cols[0].write('Credit and dollar usage by category (Previous month)')
    row1_cols[0].dataframe(prev_cost_df, use_container_width=True)
    row1_cols[1].write('Credit and dollar usage by category (Current month)')
    row1_cols[1].dataframe(curr_cost_df, use_container_width=True)
    st.write("")
    st.write("")
    st.write("")
    st.success(""" Comparison of metric from previous month.""")
    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric("Autoclustering", "5.01", "2.17")
    metric2.metric("Cloud services", "462.06", "-8%")
    metric3.metric("Compute", "2,096.21", "4%")
    metric4.metric("Storage", "0.00", "-3%")
    st.write("")
    st.write("")
    st.write("")
    st.plotly_chart(plot_total_usage(df), use_container_width=True)
    st.write("")
    st.write("")
    st.success("Total Cost in timeseries.")
    st.plotly_chart(plot_total_usage_ts(df),use_container_width=True)
