import dash
import dash_test as com
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash import Dash, html, dcc,dash_table

dash.register_page(__name__, path='/', name='Cost Analysis')

# Functions returning tables and plots for cost analysis
df_total_usage_cost=com.usage_cost()
fig1,fig2=com.cost_by_usage_plots()
df_by_user,df_low_usage_users,df_by_user_consolidated,fig3,fig4=com.cost_by_user_plots()
df_by_wh,fig_wh,fig_wh_ts=com.cost_by_warehouse_plots()
df_by_pt,fig_pt,fig_pt_ts,fig_pt_wh=com.cost_by_pt()
df_stats=com.stats()

# Dataframes that are associated with cost analysis
table_usage=dash_table.DataTable(df_total_usage_cost.to_dict('records'),id="table_usage")
table_users=dash_table.DataTable(df_by_user.to_dict('records'),id="table_users")
table_low_usage=dash_table.DataTable(df_low_usage_users.to_dict('records'),id="table_low_usage")
table_user_consolidated=dash_table.DataTable(df_by_user_consolidated.to_dict('records'),id="table_user_consolidated")
table_warehouse=dash_table.DataTable(df_by_wh.to_dict('records'),id="table_warehouse")
table_pt=dash_table.DataTable(df_by_pt.to_dict('records'),id="table_pt")
table_stats=dash_table.DataTable(df_stats.to_dict('records'),id="table_min_max")

layout = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H3("Cost by usage category"),table_usage,
                        html.H3("Statistics"),table_stats,
                        html.H3("Pie chart - cost by usage"),dcc.Graph(id="graph1", figure=fig1),
                        html.H3("Cost by usage -Time series"),dcc.Graph(id="graph2", figure=fig2)
                    ], 
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [html.H3("Cost by user(Current month)"),table_users,
                    html.H3("Low usage users"),table_low_usage,
                    html.H3("Low usage users consolidated"),table_user_consolidated,
                    html.H3("Pie chart -users"),dcc.Graph(id="graph3", figure=fig3),
                    html.H3("Time series- users"),dcc.Graph(id="graph4", figure=fig4),
                    
                    ],
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [html.H3("Cost by warehouse"),table_warehouse,
                    html.H3("Cost by warehouse plot"),dcc.Graph(id="graph_wh_", figure=fig_wh),
                    html.H3("Cost by warehouse - time series"),dcc.Graph(id="graph_ts", figure=fig_wh_ts),
                    ],
                )
            ]
        ),
           dbc.Row(
            [
                dbc.Col(
                    [html.H3("Cost by partner tools"),table_pt,html.H3("Cost of partner tools -pie chart "),dcc.Graph(id="graph_pt", figure=fig_pt),html.H3("Cost of partner tools -time series"),dcc.Graph(id="graph_pt_ts", figure=fig_pt_ts),
                    html.H3("Cost by partner tools - warehouse"),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[0]),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[1]),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[2]),dcc.Graph(id="graph_wh_", figure=fig_pt_wh[3]),
                    ],
                )
            ]
        ),
])


