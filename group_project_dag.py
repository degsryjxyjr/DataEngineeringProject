import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import zipfile
from io import BytesIO
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_history_etl',
    default_args=default_args,
    description='ETL pipeline for Global Land Temperature data with validation and trigger rules',
    schedule_interval='@daily',
)

# Paths to the CSV file and SQLite database
csv_file_directory = '/home/iit0/airflow/datasets/'
csv_file_path = csv_file_directory + "weatherHistory.csv"

db_path = '/home/iit0/airflow/databases/weatherHistory_data.db'

dataset_url = "https://www.kaggle.com/api/v1/datasets/download/muthuj7/weather-dataset"

# Task 1: Extract data
def extract_data(**kwargs):
    response = requests.get(dataset_url)

    if response.status_code == 200:
        # Create a ZipFile object from the response content
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            # Extract all contents to the current directory
            zip_file.extractall(csv_file_directory)
            print("Files extracted successfully")
    else:
        print(f"Failed to download file: {response.status_code}")

    

    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

def classify_wind(wind_speed):
        if 0 <= wind_speed <= 1.5:
            return 'Calm'
        elif 1.6 <= wind_speed <= 3.3:
            return 'Light Air'
        elif 3.4 <= wind_speed <= 5.4:
            return 'Light Breeze'
        elif 5.5 <= wind_speed <= 7.9:
            return 'Gentle Breeze'
        elif 8.0 <= wind_speed <= 10.7:
            return 'Moderate Breeze'
        elif 10.8 <= wind_speed <= 13.8:
            return 'Fresh Breeze'
        elif 13.9 <= wind_speed <= 17.1:
            return 'Strong Breeze'
        elif 17.2 <= wind_speed <= 20.7:
            return 'Near Gale'
        elif 20.8 <= wind_speed <= 24.4:
            return 'Gale'
        elif 24.5 <= wind_speed <= 28.4:
            return 'Strong Gale'
        elif 28.5 <= wind_speed <= 32.6:
            return 'Storm'
        elif wind_speed >= 32.7:
            return 'Violent Storm'

def transform_data(**kwargs):

    weather_data_dict = kwargs['ti'].xcom_pull(key='weather_data')
    df = pd.read_csv(csv_file_path)

    df["Formatted Date"] = df["Formatted Date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f %z"))



    #Daily averages calculation
    
    df["daily_temperature"] = df.groupby(df["Formatted Date"].dt.floor('d'))["Temperature (C)"].transform('mean')
    df["daily_humidity"] = df.groupby(df["Formatted Date"].dt.floor('d'))["Humidity"].transform('mean')
    df["daily_wind_speed"] = df.groupby(df["Formatted Date"].dt.floor('d'))["Wind Speed (km/h)"].transform('mean')

    #Montly mode for percipitation
    df["mode_percipitation_type"] =  df.groupby(df["Formatted Date"].dt.month)['Precipitation'].mode()

    #Wind strength categorization
    df["wind_strengths"] = df['Wind Speed (km/h)'].apply(classify_wind)
    
    # Monthly averages for temperature, humidity, wind speed, visibility, and pressure
    df["monthly_temperature"] = df.groupby(df["Formatted Date"].dt.floor('M')["Temperature (C)"]).mean()
    df["monthly_humidity"] = df.groupby(df["Formatted Date"].dt.floor('M')["Humidity"]).mean()
    df["monthly_wind_speed"] = df.groupby(df["Formatted Date"].dt.floor('M')["Wind Speed (km/h)"]).mean()
    df["monthly_visibility"] = df.groupby(df["Formatted Date"].dt.floor('M')["Visibility (km)"]).mean()
    df["monthly_pressure"] = df.groupby(df["Formatted Date"].dt.floor('M')["Pressure (millibars)"]).mean()

    df['Visibility (km)'].dropna(df['Visibility (km)'])
    df['Pressure (millibars)'].dropna(df['Pressure (millibars)'])
    
    df = df.drop_duplicates()

    df['feels_like_temperature'] = df['temperature'] + (0.33 * df['humidity']) - (0.7 * df['wind_speed']) -4

    # Save daily values to new csv file called daily_values.csv
    daily_values_file_path = '/tmp/daily_values.csv'
    df.to_csv(daily_values_file_path, index=False)

    # Save monthly values to new csv file called monthly_values.csv
    monthly_values_file_path = '/tmp/monthly_values.csv'
    df.to_csv(monthly_values_file_path, index=False)

    kwargs['ti'].xcom_push(key='transformed_file_path', value=transformed_file_path)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

