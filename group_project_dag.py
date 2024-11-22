import pandas as pd
import numpy as np
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

# Paths to the CSV file, dataset url and SQLite database
csv_file_directory = '/home/iit0/airflow/datasets/'
csv_file_path = csv_file_directory + "weatherHistory.csv"

db_path = '/home/iit0/airflow/databases/weatherHistory_data.db'

dataset_url = "https://www.kaggle.com/api/v1/datasets/download/muthuj7/weather-dataset"


# Task 1: Extract data
def extract_data(**kwargs):
    response = requests.get(dataset_url)
    # If we get OK go forward
    if response.status_code == 200:
        # Check if the content type is zip
        content_type = response.headers.get('Content-Type', '')
        # If the file is a zip file, extract it
        if 'zip' in content_type:
            # Create a ZipFile object from the response content
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                # Extract all contents to the specified directory
                zip_file.extractall(csv_file_directory)
                print("Files extracted successfully")
        # If not a zipfile download it normally
        else:
            # Write the content to a file
            with open(csv_file_path, 'wb') as f:
                f.write(response.content)
            print(f"File downloaded successfully to {csv_file_directory}")
    else:
        print(f"Failed to download file: {response.status_code}")

    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)


# Function to classify wind speeds
def classify_wind(wind_speed):
    # Convert the given km/h speed into m/s
    wind_speed = wind_speed * 1000
    wind_speed = wind_speed / 3600

    if 0 <= wind_speed <= 1.5:
        return 'Calm'
    elif 1.5 <= wind_speed <= 3.3:
        return 'Light Air'
    elif 3.3 <= wind_speed <= 5.4:
        return 'Light Breeze'
    elif 5.4 <= wind_speed <= 7.9:
        return 'Gentle Breeze'
    elif 7.9 <= wind_speed <= 10.7:
        return 'Moderate Breeze'
    elif 10.7 <= wind_speed <= 13.8:
        return 'Fresh Breeze'
    elif 13.8 <= wind_speed <= 17.1:
        return 'Strong Breeze'
    elif 17.1 <= wind_speed <= 20.7:
        return 'Near Gale'
    elif 20.7 <= wind_speed <= 24.4:
        return 'Gale'
    elif 24.4 <= wind_speed <= 28.4:
        return 'Strong Gale'
    elif 28.4 <= wind_speed <= 32.6:
        return 'Storm'
    elif wind_speed >= 32.6:
        return 'Violent Storm'


# Function to calculate the precipitation mode and if there's no clear mode return NaN
def get_precip_mode_or_nan(precip):
    # Get the frequency of each precipitation type
    value_counts = precip.value_counts()
    # Check if there's more than one maximum value
    if (value_counts == value_counts.max()).sum() > 1:
        # If there is it means the mode isn't clear and we return NaN
        return np.nan
    # Return the mode
    return value_counts.idxmax()


# Task 2 Transform data
def transform_data(**kwargs):

    # Pull the weather
    weather_data_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    # Load the csv into a dataframe
    df = pd.read_csv(weather_data_path)



    # Formatting the column into a date, eg. 2006-04-01
    df["Formatted Date"] = df["Formatted Date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f %z").date())
    # Created Month column
    df["Month"] = df["Formatted Date"].apply(lambda x: x.strftime("%Y-%m"))

    # Drop NA
    df["Formatted Date"] = df["Formatted Date"].dropna()




    # Daily averages calculation
    df["daily_avg_temperature"] = df.groupby(df["Formatted Date"])["Temperature (C)"].transform('mean')
    df["daily_avg_apparent_temperature"] = df.groupby(df["Formatted Date"])["Apparent Temperature (C)"].transform(
        "mean")
    df["daily_avg_humidity"] = df.groupby(df["Formatted Date"])["Humidity"].transform('mean')
    df["daily_avg_wind_speed"] = df.groupby(df["Formatted Date"])["Wind Speed (km/h)"].transform('mean')
    df["daily_avg_visibility"] = df.groupby(df["Formatted Date"])["Visibility (km)"].transform('mean')
    df["daily_avg_pressure"] = df.groupby(df["Formatted Date"])["Pressure (millibars)"].transform("mean")
    # Wind strength categorization, automatically converts km/h to into m/s for correct categorization
    # First we convert speeds to strenghts
    df["wind_strengths"] = df['Wind Speed (km/h)'].apply(classify_wind)
    # Next we find the wind strength mode for each day
    df["wind_strength"] = df.groupby("Formatted Date")["wind_strengths"].transform(lambda x: x.mode()[0])





    # Monthly averages calculation
    df["monthly_avg_temperature"] = df.groupby("Month")["Temperature (C)"].transform('mean')
    df["monthly_avg_apparent_temperature"] = df.groupby("Month")["Apparent Temperature (C)"].transform('mean')
    df["monthly_avg_humidity"] = df.groupby("Month")["Humidity"].transform('mean')
    df["monthly_avg_visibility"] = df.groupby("Month")["Visibility (km)"].transform('mean')
    df["monthly_avg_pressure"] = df.groupby("Month")["Pressure (millibars)"].transform('mean')
    # Monthly mode for percipitation
    df["mode_precipitation_type"] = df.groupby("Month")["Precip Type"].transform(get_precip_mode_or_nan)




    # Making daily dataframe by copying the relevant columns
    daily_weather = df[["Formatted Date", "daily_avg_temperature", "daily_avg_apparent_temperature",
                        "daily_avg_humidity", "daily_avg_wind_speed", "daily_avg_visibility",
                        "daily_avg_pressure", "wind_strength"]].drop_duplicates()

    # Making the monthly dataframe by copying the relevant columns
    monthly_weather = df[["Month", "monthly_avg_temperature", "monthly_avg_apparent_temperature",
                          "monthly_avg_humidity", "monthly_avg_visibility", "monthly_avg_pressure",
                          "mode_precipitation_type"]].drop_duplicates()



    # Sorting the dataframe by date
    daily_weather = daily_weather.sort_values(by="Formatted Date")
    # Save daily values to new csv file called daily_values.csv
    daily_values_file_path = csv_file_directory + 'daily_values.csv'
    daily_weather.to_csv(daily_values_file_path, index=False)

    # Sorting the dataframe by date
    monthly_weather = monthly_weather.sort_values(by="Month")
    # Save monthly values to new csv file called monthly_values.csv
    monthly_values_file_path = csv_file_directory + 'monthly_values.csv'
    monthly_weather.to_csv(monthly_values_file_path, index=False)

    # Push the daily.csv path to XCOM
    kwargs['ti'].xcom_push(key='daily_values_file_path', value=daily_values_file_path)
    # Push the monhtly.csv path to XCOM
    kwargs['ti'].xcom_push(key='monthly_values_file_path', value=monthly_values_file_path)


transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

