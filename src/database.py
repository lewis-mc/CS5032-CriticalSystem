from influxdb import InfluxDBClient

# Initialize InfluxDB client
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'myDB')

def reset_database(db_name='myDB'):
    """
    Drops the existing database and creates a new one with the specified name.
    """
    # Drop the database if it already exists
    databases = client.get_list_database()
    if any(db['name'] == db_name for db in databases):
        client.drop_database(db_name)
        print(f"Dropped existing database '{db_name}'.")

    # Create a new database
    client.create_database(db_name)
    print(f"Created a new database '{db_name}'.")

# Reset database
reset_database()

# Columns that are considered tags (static values, indexed for filtering)
TAG_COLUMNS = ['zip_code', 'state', 'name']

# Fields (the actual data you want to store for each measurement)
FIELD_COLUMNS = [
    'chance_of_rain', 'chance_of_snow', 'cloud', 'dewpoint_c', 'dewpoint_f',
    'feelslike_c', 'feelslike_f', 'gust_kph', 'gust_mph', 'heatindex_c', 'heatindex_f',
    'humidity', 'is_day', 'precip_in', 'precip_mm', 'pressure_in', 'pressure_mb',
    'snow_cm', 'temp_c', 'temp_f', 'uv', 'vis_km', 'vis_miles', 'will_it_rain',
    'will_it_snow', 'wind_degree', 'wind_dir', 'wind_kph', 'wind_mph', 'windchill_c', 'windchill_f',
    'lat', 'lon'
]

def insert_batch(batch_list):
    """
    Inserts a batch of weather data into InfluxDB in a modular way.
    :param batch_list: List of weather data records (dictionaries)
    :return: None
    """
    influx_data = []

    for record in batch_list:
        # Create the tags and fields dynamically
        tags = {tag: record[tag] for tag in TAG_COLUMNS if tag in record}
        fields = {}

        for field in FIELD_COLUMNS:
            # Convert the fields into appropriate types (if needed)
            if field in record:
                value = record[field]
                
                # Skip any empty or invalid data (could be NaN, None, etc.)
                if value is not None and value != '':
                    try:
                        # Convert values dynamically
                        if isinstance(value, str) and field != 'wind_dir':  # Allow wind_dir to be a string
                            value = float(value)  # Convert string numbers to float
                        fields[field] = value
                    except ValueError:
                        # Handle the case where conversion fails (skip the value)
                        print(f"Skipping invalid value for field: {field}")

        # Add the InfluxDB point for this record
        influx_point = {
            "measurement": "weather_data",
            "tags": tags,
            "fields": fields,
            "time": record['time']  # Ensure this is ISO 8601 or epoch time
        }
        influx_data.append(influx_point)

    # Write the batch data to InfluxDB
    client.write_points(influx_data)
    # print(f"Inserted {len(influx_data)} records into InfluxDB.")

# Run a test function if desired
# reset_database()  # This ensures a fresh database for each execution
