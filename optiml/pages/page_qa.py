import dash
import dash_test as com
import pandas as pd
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html,callback
from dash import Dash, html, dcc,dash_table
from datetime import datetime as dt
import dash_test as com
df_expensive_queries=com.expensive_queries()
dash.register_page(__name__, name='Query Analysis')


layout = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.DatePickerRange(
                    id='my-date-picker-range',
                    min_date_allowed=dt(2022, 9, 12),
                    max_date_allowed=dt(2022, 10, 12),
                    initial_visible_month=dt(2022, 9, 12),
                    start_date=dt(2022,9,12).date(),
                    end_date=dt(2022, 10, 12).date()
                ),
                dash_table.DataTable(
                id='datatable-interactivity',
                columns=[{"name": i, "id": i} for i in df_expensive_queries.columns],
                data=df_expensive_queries.to_dict("records")
                )
                    ], 
                )
            ]
        ),])
def date_string_to_date(date_string):
    return pd.to_datetime(date_string, infer_datetime_format=True)

@callback(
   Output("datatable-interactivity", "data"),
    [
        Input("my-date-picker-range", "start_date"),
        Input("my-date-picker-range", "end_date"),
    ],
)
def update_data(start_date, end_date):
    dff = df_expensive_queries
    dff["start_time"] = date_string_to_date(dff["start_time"]).dt.strftime("%Y/%m/%d")
    dff['start_time'] = dff['start_time'].astype('datetime64[ns]')
    dff['end_time'] = dff['end_time'].astype('datetime64[ns]')
    data = df_expensive_queries.to_dict("records")
    if start_date and end_date:
        mask = (date_string_to_date(df_expensive_queries["start_time"]) >= date_string_to_date(start_date)) & (
            date_string_to_date(df_expensive_queries["end_time"]) <= date_string_to_date(end_date)
        )
        data = dff.loc[mask].to_dict("records")
    return data