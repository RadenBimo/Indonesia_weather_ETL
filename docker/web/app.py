from dash.dependencies import Output, Input, State
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
from flask import Flask
import pandas as pd
import dash

import psycopg2
from datetime import date, timedelta
import pandas.io.sql as sqlio


server = Flask(__name__)
app = dash.Dash(server=server, external_stylesheets=[dbc.themes.FLATLY])
app.title = 'Dashboard'

conn = psycopg2.connect(
    host='postgres',
    port='5432',
    database='postgres',
    user='dataeng',
    password='thisispassword'
)
df_cols = [
    'temp', 'feelslike', 'dew', 'humidity', 'precip',
    'precipprob', 'snow', 'snowdepth', 'windgust',
    'windspeed', 'winddir', 'sealevelpressure', 'cloudcover', 'visibility',
    'solarradiation', 'solarenergy', 'uvindex', 'severerisk'
] 

citys = sorted(["DKI Jakarta","Jawa Barat","Jawa Tengah","DI Yogyakarta", "Papua"])

app.layout = dbc.Container([ 
    dbc.Row(
        dbc.Col(
            html.H2(f"Weather Dashboard"), 
            width={'size': 12, 'offset': 0, 'order': 0}), 
            style = {'textAlign': 'center', 'paddingBottom': '1%'}
    ),
    dbc.Row(
        dbc.Col(
            dcc.Loading(
                children=[
                    dcc.Dropdown(options=[{'label': city, 'value': city} for city in citys], value='DKI Jakarta', id='dropdown-city'),
                    dcc.Dropdown(options=[{'label': col, 'value': col} for col in df_cols], value='temp', id='dropdown-col'),
                    html.Div(id='weather-div'),
                    dcc.Graph(id ='data-graph'),
                    dcc.Slider(
                        id='days-slider',
                        min= 1,
                        max= 7,
                        value=1,
                        marks={str(num): str(num) for num in range(1,8)},
                        step=None
                    ),
                    dcc.Interval( 
                        id='interval-c',
                        interval=30*1000,
                        n_intervals=0
                    )
                ], color = '#000000', type = 'dot', fullscreen=True 
            ) 
        )
    )
])


@app.callback(
    Output('weather-div', 'children'),
    [Input('interval-c', 'n_intervals'),Input('dropdown-col', 'value'),Input('dropdown-city', 'value')])
def update_weather_now(n,col,city):
    city_ = city.replace(' ','')
    day_now = date.today() + timedelta(days=1)
    diff_date = date.today() - timedelta(days=1)
    df = sqlio.read_sql_query(f"""
                                WITH dw AS (
                                    SELECT datetime, conditions_id
                                    FROM {city_}_weather
                                    WHERE datetime BETWEEN '{diff_date}' AND '{day_now}'
                                    ORDER by datetime desc
                                    LIMIT 1
                                )
                                SELECT dw.datetime, c.conditions
                                FROM dw
                                INNER JOIN conditions c
                                    ON c.conditions_id = dw.conditions_id""", conn)
    return html.H3(f"{city} {df['datetime'][0].strftime('%H.%M')}: {df['conditions'].to_string(index=False)}")

@app.callback(
    Output('data-graph', 'figure'),
    [Input('interval-c', 'n_intervals'),Input('days-slider', 'value'),Input('dropdown-col', 'value'),Input('dropdown-city', 'value')])
def update_figure(n,days,col,city):
    city_= city.replace(' ','')
    day_now = date.today() + timedelta(days=1)
    diff_date = day_now - timedelta(days=days)
    df = sqlio.read_sql_query(f"""
                                SELECT 
                                datetime, {col}
                                FROM {city_}_weather 
                                WHERE datetime BETWEEN '{diff_date}' AND '{day_now}' ;
                                """, conn)

    fig = px.line(df.sort_values('datetime'), x="datetime", y=col)

    fig.update_layout(transition_duration=500)
    return fig

if __name__=='__main__':
    app.run_server()