import requests
import pandas as pd
from datetime import datetime

from pyspark import F
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, round, col, weekofyear, avg, lit


def import_requests(api_key, city):
    endpoint = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}'
    response = requests.get(endpoint)
    response.raise_for_status()
    return response.json()


def fetch_weather_data(api_key, city):
    data = import_requests(api_key, city)

    # Extract date-related weather fields
    daily_forecast_data = data['list']

    # Create lists to store extracted data
    dates, temperatures, humidities, wind_speeds, weather_descriptions = [], [], [], [], []

    for forecast in daily_forecast_data:
        timestamp = forecast['dt']
        date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        temperature = forecast['main']['temp']
        humidity = forecast['main']['humidity']
        wind_speed = forecast['wind']['speed']
        weather_description = forecast['weather'][0]['description']

        # Append data to lists
        dates.append(date)
        temperatures.append(temperature)
        humidities.append(humidity)
        wind_speeds.append(wind_speed)
        weather_descriptions.append(weather_description)

    # Extracting city and country
    city_name = data.get('city', {}).get('name', '')
    country_name = data.get('city', {}).get('country', '')

    # Extracting latitude and longitude
    latitude = data.get('city', {}).get('coord', {}).get('lat', '')
    longitude = data.get('city', {}).get('coord', {}).get('lon', '')

    # Create a Pandas DataFrame
    weather_df = pd.DataFrame({
        'country': country_name,
        'city': city_name,
        'weatherDate': dates,
        'Temperature': temperatures,
        'Humidity': humidities,
        'WindSpeed': wind_speeds,
        'WeatherDescription': weather_descriptions,
        'latitude': latitude,
        'longitude': longitude
    })

    return weather_df


def process_spark_data(spark, weather_df):
    spark_weather_df = spark.createDataFrame(weather_df)

    # Convert temperature from Kelvin to Celsius, Convert wind speed from meters per second to kilometers per hour
    spark_weather_df = spark_weather_df.withColumn('Temperature', (col('Temperature') - 273.15)).withColumn('WindSpeed',
                                                                                                            col('WindSpeed') * 3.6).withColumn(
        'jobdate', current_timestamp())

    # Round the 'Temperature' and 'WindSpeed' columns to 2 decimal places
    spark_weather_df = spark_weather_df.withColumn('Temperature_in_Celsius', round('Temperature', 2)).withColumn(
        'WindSpeed_km_per_hour', round('WindSpeed', 2)).drop('Temperature', 'WindSpeed').select('country', 'city',
                                                                                                'latitude',
                                                                                                'longitude',
                                                                                                'weatherDate',
                                                                                                'Temperature_in_Celsius',
                                                                                                'WindSpeed_km_per_hour',
                                                                                                'Humidity',
                                                                                                'WeatherDescription',
                                                                                                'jobdate')

    # Calculate the time difference in minutes
    spark_weather_df = spark_weather_df.withColumn(
        'time_diff',
        F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp('jobdate')
    )

    # Filter the DataFrame for the last one hour
    last_hour_df = spark_weather_df.filter((col('time_diff') >= 0) & (col('time_diff') <= 3600))

    # Drop the intermediate columns if needed
    spark_weather_dfh = last_hour_df.drop('time_diff')

    #spark_weather_dfh.show()

    return spark_weather_dfh


def calculate_avg_temperature(spark_weather_df):
    # Calculate average temperatures for each week
    average_temperature_per_week = (
        spark_weather_df
        .groupBy("country", "city", weekofyear('weatherDate').alias('week'))
        .agg(round(avg('Temperature_in_Celsius'), 2).alias('average_temperature'))
    )

    return average_temperature_per_week


def calculate_avg_humidity(spark_weather_df, start_date, end_date):
    # Find the average humidity for a given time period
    average_humidity_for_time_period = (
        spark_weather_df
        .filter((col('weatherDate') >= start_date) & (col('weatherDate') <= end_date))
        .groupBy("country", "city")
        .agg(
            round(avg('Humidity'), 2).alias('average_humidity')
        )
    )

    # Add start_date and end_date as constant values
    average_humidity_for_time_period = (
        average_humidity_for_time_period
        .withColumn("start_date", lit(start_date))
        .withColumn("end_date", lit(end_date))
    )

    return average_humidity_for_time_period


def main():
    api_key = '637dbddfd460f96d557f371141279b5c'
    city = 'Houston'
    start_date = '2023-11-18'
    end_date = '2023-11-19'

    spark = SparkSession.builder.appName("WeatherReportApp").getOrCreate()

    # MySQL connection properties
    weather_db_properties_url = "jdbc:mysql://localhost:3306/weather_db"
    weather_db_properties = {
        "user": "your_username",
        "password": "your_password",
        "driver": "com.mysql.jdbc.Driver",
    }

    # MySQL query to fetch data
    weather_data_query = "SELECT * FROM weather_db.weather_report_data"

    try:
        weather_df = fetch_weather_data(api_key, city)
        spark_weather_df = process_spark_data(spark, weather_df)

        # Calculate and display average temperatures for each week
        avg_temp_df = calculate_avg_temperature(spark_weather_df)
        #avg_temp_df.show()

        # Write the average temperature DataFrame to the MySQL table
        avg_temp_df.write.jdbc(url=weather_db_properties_url, table="weather_db.weekly_avg_temp_report_data",
                               mode='append',
                               properties=weather_db_properties)

        # Calculate and display average humidity for a given time period
        avg_humidity_df = calculate_avg_humidity(spark_weather_df, start_date, end_date)
        #avg_humidity_df.show(truncate=False)

        # Write the average humidity DataFrame to the MySQL table
        avg_humidity_df.write.jdbc(url=weather_db_properties_url,
                                   table="weather_db.weather_avg_humidity_report_data", mode='append',
                                   properties=weather_db_properties)

        # Read data from MySQL into a PySpark DataFrame
        weather_dest_data = spark.read.jdbc(url=weather_db_properties_url, table=weather_data_query,
                                            properties=weather_db_properties)

        # Specify the key columns for the join as a list
        join_keys = ['country', 'city', 'weatherDate']

        # Perform left anti join
        result_df = spark_weather_df.join(weather_dest_data, on=join_keys, how='left_anti')

        # Write the DataFrame to the MySQL table
        result_df.write.jdbc(url=weather_db_properties_url, table="weather_db.weather_report_data", mode='append',
                             properties=weather_db_properties)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Stop the SparkSession when done
        spark.stop()


if __name__ == "__main__":
    main()
