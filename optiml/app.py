
from dash import Dash, html, dcc,dash_table
import plotly.express as px
import pandas as pd
import pandas as pd
import hashlib
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tabulate import tabulate
color_scheme=["red","blue","green","orange","purple","brown","pink","gray","olive","cyan","darkviolet","goldenrod","darkgreen","chocolate","lawngreen"]


import dashcommands as com
df_total_usage_cost=com.usage_cost()
df_by_usage_category,df_by_category_ts=com.usage_category()



app = Dash(__name__)
title1 = html.H4("Cost entry", style={"textAlign": "left", "margin": 5})
table1=dash_table.DataTable(df_total_usage_cost.to_dict('records'),id="table")
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
my_graph1 = dcc.Graph(id="graph", figure=fig1)
fig2 = px.area(df_by_category_ts, x="hourly_start_time", y="dollars", color="category_name",color_discrete_sequence=color_scheme)
fig2.update_layout(
    title={
        'text': "Timeseries of cost by usage category",
        'y':0.95,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'})
my_graph2 = dcc.Graph(id="graph", figure=fig2)



app.layout = html.Div([title1, table1,my_graph1,my_graph2], style={"margin": 30})


if __name__ == '__main__':
    app.run_server(debug=True)