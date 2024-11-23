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


#Data cleaning
def clean_data(**kwargs):
    #double check if the file_path is needed wqith this
    file_path = kwargs["ti"].xcom_pull(key="csv_file_path")
    df = pd.read_csv(file_path)

    #date conversion
    df["Formatted Date"] = df["Formatted Date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f %z").date())

    #replace NaNs with median value
    df["Temperature (C)"].fillna(df["Temperature (C)"].median(), inplace=True)
    df["Apparent Temperature (C)"].fillna(df["Apparent Temperature (C)"].median(), inplace=True)
    df["Humidity"].fillna(df["Humidity"].median(), inplace=True)
    df["Wind Speed (km/h)"].fillna(df["Wind Speed (km/h)"].median(), inplace=True)
    df["Wind Bearing (degrees)"].fillna(df["Wind Bearing (degrees)"].median(), inplace=True)
    df["Visibility (km)"].fillna(df["Visibility (km)"].median(), inplace=True)
    df["Loud Cover"].fillna(df["Loud Cover"].median(), inplace=True)
    df["Pressure (millibars)"].fillna(df["Pressure (millibars)"].median(), inplace=True)


    #THIS MIGHT BE AN ISSUE
    if 3<df["Fromatted Date"].dt.month<10:
        if not 5<df["Temperature (C)"]<50:
            df["Temperature (C)"].fillna(df["Temperature (C)"].median(), inplace=True)
    else:
        if not -50<df["Temperature (C)"]<20:
            df["Temperature (C)"].fillna(df["Temperature (C)"].median(), inplace=True)


    if 3<df["Fromatted Date"].dt.month<10:
        if 5<df["Apparent Temperature (C)"]<50:
            df["Apparent Temperature (C)"].fillna(df["Apparent Temperature (C)"].median(), inplace=True)
    else:
        if -45<df["Apparent Temperature (C)"]<20:
            df["Apparent Temperature (C)"].fillna(df["Apparent Temperature (C)"].median(), inplace=True)


    if not 0<=df["Humidity"]<=1:
        df["Humidity"].fillna(df["Humidity"].median(), inplace=True)

    if not 0<=df["Wind Speed (km/h)"]<=408:
        df["Wind Speed (km/h)"].fillna(df["Wind Speed (km/h)"].median(), inplace=True)

    if not 0<=df["Visibility (km)"]:
        df["Visibility (km)"].fillna(df["Visibility (km)"].median(), inplace=True)

    if not 870<=df["Pressure (millibars)"]<=1083.8:
        df["Pressure (millibars)"].fillna(df["Pressure (millibars)"].median(), inplace=True)


    df.drop_duplicates(subset=["Formatted Date"], keep='first')

cleaining_task = PythonOperator(
    task_id='cleaning_task',
    python_callable=clean_data,
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
#    df["Formatted Date"] = df["Formatted Date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f %z").date())
    # Created Month column
    df["Month"] = df["Formatted Date"].apply(lambda x: x.strftime("%Y-%m"))

    # Drop NA
    df["Formatted Date"] = df["Formatted Date"].dropna()




    # Daily averages calculation
    df["daily_avg_temperature"] = df.groupby(df["Formatted Date"])["Temperature (C)"].transform('mean')
#    df["daily_avg_apparent_temperature"] = df.groupby(df["Formatted Date"])["Apparent Temperature (C)"].transform("mean")
    df["daily_avg_humidity"] = df.groupby(df["Formatted Date"])["Humidity"].transform('mean')
    df["daily_avg_wind_speed"] = df.groupby(df["Formatted Date"])["Wind Speed (km/h)"].transform('mean')
#    df["daily_avg_visibility"] = df.groupby(df["Formatted Date"])["Visibility (km)"].transform('mean')
#    df["daily_avg_pressure"] = df.groupby(df["Formatted Date"])["Pressure (millibars)"].transform("mean")
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





#DATA VALIDATION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!






def load_data(**kwargs):
    validated_history=kwargs["ti"].xcom_pull(key="PLACEHOLDER_FOR_DATA_VALIDATION")
    validated_daily=kwargs["ti"].xcom_pull(key="PLACEHOLDER_FOR_DATA_VALIDATION")
    validated_monthly=kwargs["ti"].xcom_pull(key="PLACEHOLDER_FOR_DATA_VALIDATION")
    df=pd.read_csv(validated_history)
    df1=pd.read_csv(validated_daily)
    df2=pd.read_csv(validated_monthly)

    conn=sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if it does not exist
    #what to store formatted date as since i dont think it had timezones
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_history (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Formatted Date" DATETIME,
            "Summary" VARCHAR(100),
            "Precip Type" VARCHAR(50),
            "Temperature (C)" FLOAT,
            "Apparent Temperature (C)" FLOAT,
            "Humidity" FLOAT,
            "Wind Speed (km/h)" FLOAT,
            "Wind Bearing (degrees)" FLOAT,
            "Visibility (km)" FLOAT,
            "Loud Cover" FLOAT,
            "Pressure (millibars)" FLOAT,
            "Daily Summary" VARCHAR(300)
        )
        '''
    )

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Formatted Date" DATETIME,
            "Temperature (C)" FLOAT,
            "Apparent Temperature (C)" FLOAT,
            "Humidity" FLOAT,
            "Wind Speed (km/h)" FLOAT,
            "Visibility (km)" FLOAT,
            "Pressure (millibars)" FLOAT,
            "wind_strength" VARCHAR(60),
            "daily_avg_temperature" FLOAT,
            "daily_avg_humidity" FLOAT,
            "daily_avg_wind_speed" FLOAT
        )
        ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_weather (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
            "Month" INTEGER,
            "monthly_avg_temperature" FLOAT,
            "monthly_avg_apparent_temperature" FLOAT,
            "monthly_avg_humidity" FLOAT,
            "monthly_avg_visibility" FLOAT,
            "monthly_avg_pressure" FLOAT,
            "mode_precipitation_type" VARCHAR(50)
        )
        ''')



    # Insert data into the database
    df.to_sql('weather_history', conn, if_exists='append', index=False)
    df1.to_sql('daily_weather', conn, if_exists='append', index=False)
    df2.to_sql('monthly_weather', conn, if_exists='append', index=False)
    conn.commit()
    conn.cursor()
    conn.close()




load_task =PythonOperator(
    task_id="load_task",
    python_callable=load_data,
    provide_context=True, 
    dag=dag,
    trigger_rule="all_success",
)