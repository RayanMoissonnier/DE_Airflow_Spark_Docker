from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import duckdb

def fetch_weather_data(**kwargs):
    url = 'https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m'
    response = requests.get(url)
    data = response.json()
    df = pd.json_normalize(data['hourly'], sep='_')
    df.to_csv('/tmp/weather_data.csv', index=False)

def process_data_spark(**kwargs):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("WeatherDataProcessing") \
        .getOrCreate()
    df = spark.read.csv('/tmp/weather_data.csv', header=True, inferSchema=True)
    df.createOrReplaceTempView("weather")
    processed_df = spark.sql("""
        SELECT
            *,
            CASE
                WHEN temperature_2m > 30 THEN 'Hot'
                ELSE 'Moderate'
            END AS temperature_label
        FROM weather
    """)
    processed_df.write.mode('overwrite').parquet('/tmp/processed_weather_data.parquet')
    spark.stop()

def load_data_duckdb(**kwargs):
    con = duckdb.connect('/tmp/weather_data.duckdb')
    con.execute("""
        CREATE TABLE IF NOT EXISTS weather AS 
        SELECT * FROM read_parquet('/tmp/processed_weather_data.parquet')
    """)
    con.close()

default_args = {
    'owner': 'Rayan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='DAG to fetch, process, and load weather data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 6, 10, 2),  # Début au 10 juin 2024
    catchup=True,
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    process_data = PythonOperator(
        task_id='process_data_spark',
        python_callable=process_data_spark
    )

    load_data = PythonOperator(
        task_id='load_data_duckdb',
        python_callable=load_data_duckdb
    )

    task_echo = BashOperator(
        task_id='echo_completion',
        bash_command="echo Data pipeline completed successfully!"
    )

    fetch_data >> process_data >> load_data >> task_echo
