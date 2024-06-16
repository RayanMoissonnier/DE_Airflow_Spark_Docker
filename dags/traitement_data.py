from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import duckdb
import json

default_args = {
    'owner': 'Rayan',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_pipeline_v8',
    default_args=default_args,
    description='A simple pipeline to fetch and process weather data',
    schedule_interval=timedelta(days=1),
)

def fetch_weather_data(**kwargs):
    logical_date = kwargs['logical_date'].strftime('%Y%m%d')
    url = 'https://api.open-meteo.com/v1/forecast?latitude=45.75&longitude=4.85&hourly=temperature_2m'
    response = requests.get(url)
    data = response.json()
    
    hourly_data = data['hourly']['temperature_2m']
    timestamps = data['hourly']['time']
    latitude = data['latitude']
    longitude = data['longitude']
    
    df = pd.DataFrame({
        'timestamp': timestamps,
        'temperature_2m': hourly_data,
        'latitude': [latitude] * len(timestamps),
        'longitude': [longitude] * len(timestamps)
    })
    
    csv_path = f'/tmp/weather_data_{logical_date}.csv'
    json_path = f'/tmp/weather_data_{logical_date}.json'
    df.to_csv(csv_path, index=False)
    with open(json_path, 'w') as f:
        json.dump(data, f)

def display_csv_head(**kwargs):
    logical_date = kwargs['logical_date'].strftime('%Y%m%d')
    csv_path = f'/tmp/weather_data_{logical_date}.csv'
    try:
        df = pd.read_csv(csv_path)
        print(df.head())
    except Exception as e:
        print(f"Error reading the CSV file: {e}")

def process_data_spark(**kwargs):
    from pyspark.sql import SparkSession
    import logging
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logical_date = kwargs['logical_date'].strftime('%Y%m%d')
    csv_path = f'/tmp/weather_data_{logical_date}.csv'
    parquet_path = f'/tmp/processed_weather_data_{logical_date}.parquet'
    
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("WeatherDataProcessing") \
        .getOrCreate()
    
    logger.info("Reading CSV file...")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    logger.info("Displaying DataFrame schema...")
    df.printSchema()
    
    logger.info("Displaying first few rows of DataFrame...")
    df.show()
    
    df.createOrReplaceTempView("weather")
    
    logger.info("Verifying table creation...")
    tables = spark.sql("SHOW TABLES").show()
    logger.info(f"Tables: {tables}")
    
    logger.info("Selecting necessary columns...")
    processed_df = spark.sql("""
        SELECT
            latitude,
            longitude,
            timestamp,
            temperature_2m
        FROM weather
    """)
    
    logger.info("Writing processed data to Parquet...")
    processed_df.write.mode('overwrite').parquet(parquet_path)
    
    logger.info("Stopping Spark session...")
    spark.stop()

def load_data_duckdb(**kwargs):
    logical_date = kwargs['logical_date'].strftime('%Y%m%d')
    parquet_path = f'/tmp/processed_weather_data_{logical_date}.parquet'
    duckdb_path = f'/tmp/weather_data_{logical_date}.duckdb'
    con = duckdb.connect(duckdb_path)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS weather AS 
        SELECT latitude, longitude, timestamp, temperature_2m FROM read_parquet('{parquet_path}')
    """)
    con.close()

fetch_data = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

display_csv = PythonOperator(
    task_id='display_csv_head',
    python_callable=display_csv_head,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data_spark',
    python_callable=process_data_spark,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data_duckdb',
    python_callable=load_data_duckdb,
    dag=dag,
)

fetch_data >> display_csv >> process_data >> load_data
