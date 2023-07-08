from airflow import DAG
from airflow.models.param import Param
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import polars as pl
import pandas as pd
import json



dag = DAG(
    'weather_project',
    default_args={
        'start_date': datetime(2023, 7, 8),
        'owner': 'user',
        'retry_delay': timedelta(hours=1),
        'retries': 1,
        'catchup': False
    },
    params={
        'start_date': None,
        'end_date': None
    },
    catchup=False,
    schedule_interval='10 * * * *'
)

def create_db_weather(city):
    """
    membuat database weather tiap provinsi jika belum ada
    """
    city = city.replace(" ","")
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.run(
            f"""
        CREATE TABLE IF NOT EXISTS {city}_weather (
            datetime TIMESTAMP  PRIMARY KEY,
            temp FLOAT,
            feelslike FLOAT,
            dew FLOAT,
            humidity FLOAT,
            precip FLOAT,
            precipprob FLOAT,
            preciptype_id VARCHAR(100),
            snow FLOAT,
            snowdepth FLOAT,
            windgust FLOAT,
            windspeed FLOAT,
            winddir FLOAT,
            sealevelpressure FLOAT,
            cloudcover FLOAT,
            visibility FLOAT,
            solarradiation FLOAT,
            solarenergy FLOAT,
            uvindex FLOAT,
            severerisk FLOAT,
            conditions_id VARCHAR(100),
            icon_id VARCHAR(100),
            stations_id VARCHAR(100);
        
        CREATE INDEX idx_{city}_weather ON {city}_weather (datetime);
        )
    """
    )

def create_db_preciptype():
    """
    membuat database preciptype
    """
    hook  = PostgresHook(postgres_conn_id='postgres')
    hook.run(
            f"""
        CREATE SEQUENCE IF NOT EXISTS preciptype_seq;
        CREATE TABLE IF NOT EXISTS preciptype (
            preciptype_id VARCHAR(100) PRIMARY KEY DEFAULT 'pt'||nextval('preciptype_seq'::VARCHAR),
            preciptype VARCHAR(100) UNIQUE
        );
    """
    )

def create_db_stations():
    """
    membuat database preciptype
    """
    hook  = PostgresHook(postgres_conn_id='postgres')
    hook.run(
            f"""
        CREATE SEQUENCE IF NOT EXISTS stations_seq;
        CREATE TABLE IF NOT EXISTS stations (
            stations_id VARCHAR(100) PRIMARY KEY DEFAULT 'st'||nextval('stations_seq'::VARCHAR),
            stations VARCHAR(100) UNIQUE
        );
    """
    )
    
def create_db_icon():
    """
    membuat database preciptype
    """
    hook  = PostgresHook(postgres_conn_id='postgres')
    hook.run(
            f"""
        CREATE SEQUENCE IF NOT EXISTS icon_seq;
        CREATE TABLE IF NOT EXISTS icon (
            icon_id VARCHAR(100) PRIMARY KEY DEFAULT 'ic'||nextval('icon_seq'::VARCHAR),
            icon VARCHAR(100) UNIQUE
        );
    """
    )

def create_db_conditions():
    """
    membuat database preciptype
    """
    hook  = PostgresHook(postgres_conn_id='postgres')
    hook.run(
            f"""
        CREATE SEQUENCE IF NOT EXISTS conditions_seq;
        CREATE TABLE IF NOT EXISTS conditions (
            conditions_id VARCHAR(100) PRIMARY KEY DEFAULT 'con'||nextval('conditions_seq'::VARCHAR),
            conditions VARCHAR(100) UNIQUE
        );
    """
    )

def fetch_data(city,**context):    
    """
    Memanggil data dari covid API dan melakukan fetch data
    """
    city_ = city.replace(" ", "%20")
    api_key = Variable.get("API_KEY")
    start_date = context["params"]["start_date"]
    end_date = context["params"]["end_date"]
    if start_date is None:
        start_date = context['execution_date'].strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    df = pd.read_csv(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city_}/{start_date}/{end_date}?unitGroup=us&include=hours&key={api_key}&contentType=csv")
    context['ti'].xcom_push(key=f'data_frame_{city.replace(" ","")}', value=df)
     
def transform_data(city,**context):
    """
    filter data berdasarkan kota Indonesia
    transform data waktu
    """
    df = context['ti'].xcom_pull(key=f'data_frame_{city.replace(" ","")}')
    df = pl.from_pandas(df)
    
    df = (
        df
        .with_columns(
            pl.col('datetime')
            .str.strptime(pl.Datetime,
                        fmt="%Y-%m-%dT%H:%M:%S%.f",
                        strict=False)
        )
        .drop('name')
    )
    context['ti'].xcom_push(key=f'filtered_df_{city.replace(" ","")}',value=df.to_pandas())

def load_data_precipt(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    df = context['ti'].xcom_pull(key=f'filtered_df_{city.replace(" ","")}')
    hook  = PostgresHook(postgres_conn_id='postgres')        
    for index, row in df.iterrows(): 
        hook.run(f"""
            INSERT INTO preciptype (preciptype)
            VALUES ('{row['preciptype']}')
            ON CONFLICT (preciptype) DO NOTHING
        """
        )

def load_data_icon(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    df = context['ti'].xcom_pull(key=f'filtered_df_{city.replace(" ","")}')
    hook  = PostgresHook(postgres_conn_id='postgres')        
    temp_df = hook.get_pandas_df("SELECT * FROM icon;")
    new_df = (
        df
        .drop_duplicates(subset=['icon'])
        .merge(temp_df, on='icon', how='left', indicator=True)
    )
    print(new_df[new_df['_merge'] == 'left_only'])
    for index, row in new_df[new_df['_merge'] == 'left_only'].iterrows(): 
        hook.run(f"""
            INSERT INTO icon (icon)
            VALUES ('{row['icon']}')
            ON CONFLICT (icon) DO NOTHING
        """
        )

def load_data_conditions(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    df = context['ti'].xcom_pull(key=f'filtered_df_{city.replace(" ","")}')
    hook  = PostgresHook(postgres_conn_id='postgres')        
    temp_df = hook.get_pandas_df("SELECT * FROM conditions;")
    new_df = (
        df
        .drop_duplicates(subset=['conditions'])
        .merge(temp_df, on='conditions', how='left', indicator=True)
    )
    print(new_df[new_df['_merge'] == 'left_only'])
    for index, row in new_df[new_df['_merge'] == 'left_only'].iterrows(): 
        hook.run(f"""
            INSERT INTO conditions (conditions)
            VALUES ('{row['conditions']}')
            ON CONFLICT (conditions) DO NOTHING
        """
        )

def load_data_precipt(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    df = context['ti'].xcom_pull(key=f'filtered_df_{city.replace(" ","")}')
    hook  = PostgresHook(postgres_conn_id='postgres')        
    temp_df = hook.get_pandas_df("SELECT * FROM preciptype;")
    new_df = (
        df
        .drop_duplicates(subset=['preciptype'])
        .merge(temp_df, on='preciptype', how='left', indicator=True)
    )
    print(new_df[new_df['_merge'] == 'left_only'])
    for index, row in new_df[new_df['_merge'] == 'left_only'].iterrows(): 
        hook.run(f"""
            INSERT INTO preciptype (preciptype)
            VALUES ('{row['preciptype']}')
            ON CONFLICT (preciptype) DO NOTHING
        """
        )

def load_data_stations(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    df = context['ti'].xcom_pull(key=f'filtered_df_{city.replace(" ","")}')
    hook  = PostgresHook(postgres_conn_id='postgres')        
    temp_df = hook.get_pandas_df("SELECT * FROM stations;")
    new_df = (
        df
        .drop_duplicates(subset=['stations'])
        .merge(temp_df, on='stations', how='left', indicator=True)
    )
    print(new_df[new_df['_merge'] == 'left_only'])
    for index, row in new_df[new_df['_merge'] == 'left_only'].iterrows(): 
        hook.run(f"""
            INSERT INTO stations (stations)
            VALUES ('{row['stations']}')
            ON CONFLICT (stations) DO NOTHING
        """
        )

def final_transform_data(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    df = pl.from_pandas(context['ti'].xcom_pull(key=f'filtered_df_{city.replace(" ","")}'))
    hook  = PostgresHook(postgres_conn_id='postgres')        

    for key_col in ['conditions', 'icon', 'preciptype', 'stations']:
        print(key_col)
        df2 = pl.from_pandas(hook.get_pandas_df(f"SELECT * FROM {key_col};"))
        df = df.with_columns(
            df.join(
            df2, 
            left_on=key_col,
            right_on=key_col, 
            how='left')
        [f'{key_col}_id']
        ).drop(key_col)
        
    context['ti'].xcom_push(key=f'final_df_{city.replace(" ","")}',value=df.to_pandas())


def load_data(city,**context):
    """
    insert data kedalam table jika tanggal yang ada belum ada pada database
    """
    city = city.replace(" ","")
    df = context['ti'].xcom_pull(key=f'final_df_{city}')
    hook  = PostgresHook(postgres_conn_id='postgres')
    
    for index, row in df.iterrows():        
        hook.run(f"""
            INSERT INTO {city}_weather (
                datetime, temp, feelslike, dew, humidity, precip, precipprob,
                preciptype_id, snow, snowdepth, windgust, windspeed, winddir,
                sealevelpressure, cloudcover, visibility, solarradiation,
                solarenergy, uvindex, severerisk, conditions_id, icon_id, stations_id
            )
            VALUES (
                '{row['datetime']}', {row['temp']}, {row['feelslike']}, {row['dew']},
                {row['humidity']}, {row['precip']}, {row['precipprob']}, '{row['preciptype_id']}',
                {row['snow']}, {row['snowdepth']}, {row['windgust']}, {row['windspeed']},
                {row['winddir']}, {row['sealevelpressure']}, {row['cloudcover']},
                {row['visibility']}, {row['solarradiation']}, {row['solarenergy']},
                {row['uvindex']}, {row['severerisk']}, '{row['conditions_id']}', '{row['icon_id']}',
                '{row['stations_id']}'
            )
            ON CONFLICT (datetime) DO NOTHING
        """
        )


citys = Variable.get('city_list').split(',')

for city in citys:
    extract = PythonOperator(
        task_id=f'Extract_{city.replace(" ","")}',
        provide_context=True,
        python_callable=fetch_data,
        op_kwargs={'city': city},
        dag=dag
    )

    transform = PythonOperator(
        task_id=f'Transform_{city.replace(" ","")}',
        provide_context=True,
        python_callable=transform_data,
        op_kwargs={'city': city},
        dag=dag
    )

    create_weather = PythonOperator(
        task_id=f'Create_db_weather_{city.replace(" ","")}',
        provide_context=True,
        python_callable=create_db_weather,
        op_kwargs={'city': city},
        dag=dag
    )

    create_precip = PythonOperator(
        task_id=f'Create_db_precip_{city.replace(" ","")}',
        provide_context=True,
        python_callable=create_db_preciptype,
        dag=dag
    )

    create_stations = PythonOperator(
        task_id=f'Create_db_stations_{city.replace(" ","")}',
        provide_context=True,
        python_callable=create_db_stations,
        dag=dag
    )

    create_icon = PythonOperator(
        task_id=f'Create_db_icon_{city.replace(" ","")}',
        provide_context=True,
        python_callable=create_db_icon,
        dag=dag
    )

    create_conditions = PythonOperator(
        task_id=f'Create_conditions_{city.replace(" ","")}',
        provide_context=True,
        python_callable=create_db_conditions,
        dag=dag
    )

    load_precip = PythonOperator(
        task_id=f'Load_precip_{city.replace(" ","")}',
        provide_context=True,
        python_callable=load_data_precipt,
        op_kwargs={'city': city},
        dag=dag
    )

    load_conditions = PythonOperator(
        task_id=f'Load_conditions_{city.replace(" ","")}',
        provide_context=True,
        python_callable=load_data_conditions,
        op_kwargs={'city': city},
        dag=dag
    )

    load_icon = PythonOperator(
        task_id=f'Load_icon_{city.replace(" ","")}',
        provide_context=True,
        python_callable=load_data_icon,
        op_kwargs={'city': city},
        dag=dag
    )

    load_stations = PythonOperator(
        task_id=f'Load_stations_{city.replace(" ","")}',
        provide_context=True,
        python_callable=load_data_stations,
        op_kwargs={'city': city},
        dag=dag
    )

    final_transform = PythonOperator(
        task_id=f'Final_transform_{city.replace(" ","")}',
        provide_context=True,
        python_callable=final_transform_data,
        op_kwargs={'city': city},
        dag=dag
    )

    load = PythonOperator(
        task_id=f'Load_{city.replace(" ","")}',
        provide_context=True,
        python_callable=load_data,
        op_kwargs={'city': city},
        dag=dag
    )
    
    [create_precip, create_stations, create_weather, create_icon, create_conditions]>>extract>>transform>>[load_precip, load_icon, load_conditions, load_stations]>>final_transform>>load