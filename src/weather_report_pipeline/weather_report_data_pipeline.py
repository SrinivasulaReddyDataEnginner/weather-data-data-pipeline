import requests
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import round, col, weekofyear, avg, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Replace 'YOUR_API_KEY' and 'CITY_NAME' with your actual API key and location details
api_key = '637dbddfd460f96d557f371141279b5c'
city = 'Houston'

spark = SparkSession.builder \
    .appName("WeatherReportApp") \
    .getOrCreate()

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
    # API endpoint for daily forecast weather data
    endpoint = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}'

    # Make the API request
    response = requests.get(endpoint)

    # Check if the request was successful (status code 200)
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()

    # Extract date-related weather fields
    daily_forecast_data = data['list']

    # Create lists to store extracted data
    dates = []
    temperatures = []
    humidities = []
    wind_speeds = []
    weather_descriptions = []

    for forecast in daily_forecast_data:
        timestamp = forecast['dt']  # Timestamp in seconds since Epoch
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
        city = data.get('city', {}).get('name', '')
        country = data.get('city', {}).get('country', '')

    # Create a Pandas DataFrame
    weather_df = pd.DataFrame({
        'country': country,
        'city': city,
        'weatherDate': dates,
        'Temperature': temperatures,
        'Humidity': humidities,
        'WindSpeed': wind_speeds,
        'WeatherDescription': weather_descriptions

    })

    # Display the DataFrame
    # print(weather_df)

    spark_weather_df = spark.createDataFrame(weather_df)

    # Convert temperature from Kelvin to Celsius ,Convert wind speed from meters per second to kilometers per hour

    spark_weather_df = spark_weather_df.withColumn('Temperature', (col('Temperature') - 273.15)).withColumn('WindSpeed',
                                                                                                            col('WindSpeed') * 3.6).withColumn(
        'jobdate', current_timestamp())

    # Round the 'Temperature' and 'WindSpeed' columns to 2 decimal places
    spark_weather_df = spark_weather_df.withColumn('Temperature_in_Celsius', round('Temperature', 2)).withColumn(
        'WindSpeed_km_per_hour', round('WindSpeed', 2)).drop('Temperature', 'WindSpeed').select('country', 'city',
                                                                                                'weatherDate',
                                                                                                'Temperature_in_Celsius',
                                                                                                'WindSpeed_km_per_hour',
                                                                                                'Humidity',
                                                                                                'WeatherDescription',
                                                                                                'jobdate')

    # Display the PySpark DataFrame along with schema
    spark_weather_df.show()
    spark_weather_df.printSchema()



    # Calculate average temperatures for each week
    average_temperature_per_week = (
        spark_weather_df
        .groupBy("country", "city", weekofyear('weatherDate').alias('week'))
        .agg(round(avg('Temperature_in_Celsius'), 2).alias('average_temperature'))
    )

    # Show the result
    average_temperature_per_week.show()

    # Find the average humidity for a given time period
    # Replace 'start_date' and 'end_date' with the desired time period
    start_date = '2023-11-18'
    end_date = '2023-11-19'

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

    # Show the result
    average_humidity_for_time_period.show(truncate=False)



    # Read data from MySQL into a PySpark DataFrame
    weather_dest_data = spark.read.jdbc(url=weather_db_properties_url, table=weather_data_query, properties=weather_db_properties)

    # Specify the key columns for the join as a list
    join_keys = ['country', 'city', 'weatherDate']

    # Perform left anti join
    result_df = spark_weather_df.join(weather_dest_data, on=join_keys, how='left_anti')

    # Write the DataFrame to the MySQL table
    result_df.write.jdbc(url=weather_db_properties_url, table="weather_db.weather_report_data", mode='append', properties=weather_db_properties)


except Exception as e:
    print(f"Error: {e}")

finally:
    # Stop the SparkSession when done
    spark.stop()

