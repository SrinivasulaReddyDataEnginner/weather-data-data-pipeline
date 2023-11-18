import mysql.connector
from mysql.connector import Error

# Replace these values with your MySQL connection details
host = "localhost"
user = "weatherreport"
password = "weatherreport"
database = "weather_db"

try:
    # Connect to MySQL
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    # Create a cursor object
    cursor = connection.cursor()

    # Define the table creation SQL statement
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_db.weather_report_data (
        country VARCHAR(255),
        city VARCHAR(255),
        weatherDate DATE,
        Temperature DOUBLE,
        Humidity INT,
        WindSpeed DOUBLE,
        WeatherDescription VARCHAR(255),
        jobDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    # Execute the table creation SQL statement
    cursor.execute(create_table_query)

    # Commit the changes
    connection.commit()

except Error as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection in the finally block to ensure it happens even if an exception occurs
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed")
