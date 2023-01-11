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
import dashcommands as com
color_scheme=["red","blue","green","orange","purple","brown","pink","gray","olive","cyan","darkviolet","goldenrod","darkgreen","chocolate","lawngreen"]


df_total_usage_cost=com.usage_cost()
fig1,fig2=com.cost_analysis_plots()

table1=dash_table.DataTable(df_total_usage_cost.to_dict('records'),id="table")

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
        return[html.H3("Cost by usage"),table1,html.H3("Pie chart credits and dollars"),dcc.Graph(id="graph1", figure=fig1),html.H3("Time series graph"),dcc.Graph(id="graph2", figure=fig2)]
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