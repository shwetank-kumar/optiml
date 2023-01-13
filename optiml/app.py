import dash_auth
from users import USERNAME_PASSWORD_PAIRS
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash import Dash, html, dcc,dash_table
import plotly.express as px
import pandas as pd
import pandas as pd
import hashlib
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tabulate import tabulate
import dash_test as com
color_scheme=["red","blue","green","orange","purple","brown","pink","gray","olive","cyan","darkviolet","goldenrod","darkgreen","chocolate","lawngreen"]


df_total_usage_cost=com.usage_cost()
fig1,fig2=com.cost_by_usage_plots()
df_by_user,df_low_usage_users,df_by_user_consolidated,fig3,fig4=com.cost_by_user_plots()
df_by_wh,fig_wh,fig_wh_ts=com.cost_by_warehouse_plots()
df_by_pt,fig_pt,fig_pt_ts,fig_pt_wh=com.cost_by_pt()
df_stats=com.stats()
##DATAFRAMES

table_usage=dash_table.DataTable(df_total_usage_cost.to_dict('records'),id="table_usage")
table_users=dash_table.DataTable(df_by_user.to_dict('records'),id="table_users")
table_low_usage=dash_table.DataTable(df_low_usage_users.to_dict('records'),id="table_low_usage")
table_user_consolidated=dash_table.DataTable(df_by_user_consolidated.to_dict('records'),id="table_user_consolidated")
table_warehouse=dash_table.DataTable(df_by_wh.to_dict('records'),id="table_warehouse")
table_pt=dash_table.DataTable(df_by_pt.to_dict('records'),id="table_pt")
table_stats=dash_table.DataTable(df_stats.to_dict('records'),id="table_min_max")

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

auth = dash_auth.BasicAuth(
    app,
    USERNAME_PASSWORD_PAIRS
)

# the style arguments for the sidebar. We use position:fixed and a fixed width
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

sidebar = html.Div(
    [
        html.H4("Optiml",style={'textAlign':'left','verticalAlign':'top','top': 0,'padding':0}),
        html.Hr(),
        html.P(
            "Analysis Category Wise", className="lead"
        ),
        dbc.Nav(
            [
                dbc.NavLink("Cost Analysis", href="/", active="exact"),
                dbc.NavLink("Query Analysis", href="/page-1", active="exact"),
                dbc.NavLink("Warehouse Analysis", href="/page-2", active="exact"),
                dbc.NavLink("Storage Analysis", href="/page-3", active="exact"),
                dbc.NavLink("User Analysis", href="/page-4", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content",children=[], style=CONTENT_STYLE)

app.layout = html.Div([dcc.Location(id="url"),html.H3(children='Kiva', style={
        'textAlign': 'right',
        "margin-right": "2rem","margin-top":"2rem"}), sidebar, content])


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):

    if pathname == "/":
        return[html.H3("Cost by usage category"),table_usage,html.H3("Statistics"),table_stats,html.H3("Pie chart - cost by usage"),dcc.Graph(id="graph1", figure=fig1),html.H3("Cost by usage -Time series"),dcc.Graph(id="graph2", figure=fig2),
        html.H3("Cost by user(Current month)"),table_users,html.H3("Low usage users"),table_low_usage,html.H3("Low usage users consolidated"),table_user_consolidated,html.H3("Pie chart -users"),dcc.Graph(id="graph3", figure=fig3),html.H3("Time series- users"),dcc.Graph(id="graph4", figure=fig4),html.H3("Cost by warehouse"),
        table_warehouse,html.H3("Cost by warehouse plot"),dcc.Graph(id="graph_wh_", figure=fig_wh),html.H3("Cost by warehouse - time series"),dcc.Graph(id="graph_ts", figure=fig_wh_ts),html.H3("Cost by partner tools"),table_pt,html.H3("Cost of partner tools -pie chart "),dcc.Graph(id="graph_pt", figure=fig_pt),html.H3("Cost of partner tools -time series"),dcc.Graph(id="graph_pt_ts", figure=fig_pt_ts),
        html.H3("Cost by partner tools - warehouse"),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[0]),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[1]),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[2]),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[3]),

        ]
    elif pathname == "/page-1":
        return html.P("This is the content of page 1. Yay!")
    elif pathname == "/page-2":
        return html.P("Oh cool, this is page 2!")
    elif pathname == "/page-3":
        return html.P("Oh cool, this is page 3!")
    elif pathname == "/page-4":
        return html.P("Oh cool, this is page 4!")



    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )


if __name__ == "__main__":
    app.run_server(port=8888)