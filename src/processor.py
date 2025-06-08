from influxdb import InfluxDBClient
import redis
import logging
from time import sleep
import os
import threading
import queue

# Initialize logging
logging.basicConfig(filename="logs.csv", level=logging.INFO, format="%(asctime)s, %(message)s")

# Initialize Redis client and InfluxDB client
r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'myDB')

pending_data = queue.Queue()

def process_hourly_data(hour):
    """
    Calculate hourly analytics and attempt to store them in a separate measurement in InfluxDB.
    """
    start_time = f"2023-09-19T{hour:02d}:00:00Z"
    end_time = f"2023-09-19T{hour + 1:02d}:00:00Z"

    # Process state averages
    state_averages = calculate_state_averages(start_time, end_time)

    if hour < 10:
        hour = f"0{hour}"

    if state_averages:
        send_analytics_to_influxdb(hour, state_averages, "weather_averages")
    else:
        print("No state averages data available")

    # Process zip code extremes
    zip_extremes = process_zip_extremes_by_state(start_time, end_time)
    if zip_extremes:
        send_analytics_to_influxdb(hour, zip_extremes, "zip_code_extremes")

def calculate_state_averages(start_time, end_time):
    """
    Calculate average temperature, pressure, humidity, and precipitation per state.
    
    Parameters:
        start_time (str): The start time in ISO 8601 format.
        end_time (str): The end time in ISO 8601 format.
        
    Returns:
        list: A list of dictionaries containing average metrics for each state.
    """
    metrics = ['temp_c', 'pressure_mb', 'humidity', 'precip_mm']

    queries = get_queries_state_averages(metrics, start_time, end_time)
    state_averages = get_state_averages(queries, client, metrics)
 
    return state_averages


def get_queries_state_averages(metrics, start_time, end_time):
    """
    Generate queries to calculate average metrics per state.
    
    Parameters:
        metrics (list): List of metrics to calculate averages for.
        start_time (str): The start time in ISO 8601 format.
        end_time (str): The end time in ISO 8601 format.
        
    Returns:
        list: A list of InfluxDB queries.
    """
    queries = []
    for metric in metrics:
        query = f"""
        SELECT MEAN({metric}) AS avg_{metric} FROM weather_data
        WHERE time = '{start_time}'
        GROUP BY state
        """
        queries.append(query)
    return queries


def get_state_averages(queries, client, metrics):
    """
    Fetch the results of state average queries from InfluxDB.
    
    Parameters:
        queries (list): List of InfluxDB queries for state averages.
        client (InfluxDBClient): The InfluxDB client instance.
        metrics (list): List of metrics being queried.
        
    Returns:
        list: A list of dictionaries containing average values for each state.
    """
    state_averages = []
    try:
        for metric, query in zip(metrics, queries):
            
            result = client.query(query)
            for group_key, points in result.items():
                
                state = group_key[1].get('state')
                points_list = list(points)

                for points in points_list:
                    avg_value = points.get(f"avg_{metric}")
                    state_averages.append({
                            "state": state,
                            f"avg_{metric}": avg_value
                    })
        return state_averages
    except Exception as e:
        logging.error(f"PROCESSOR: InfluxDB query failed: {e}")
        return []


def process_zip_extremes_by_state(start_time, end_time):
    """
    Process zip codes with the lowest and highest metrics (temperature, pressure, humidity, precipitation)
    within a given state.
    """
    metrics = ['temp_c', 'pressure_mb', 'humidity', 'precip_mm']
    states = get_all_states()  # Retrieves all states, including DC
    zip_extremes = []

    for state in states:
        min_queries, max_queries = get_queries_zip_extremes(metrics, start_time, end_time, state)
        extremes = calculate_extremes(min_queries, max_queries, client, metrics)
        for extreme in extremes:
            extreme["state"] = state  # Add the state context to each extreme
        zip_extremes.extend(extremes)
    return zip_extremes


def get_queries_zip_extremes(metrics, start_time, end_time, state):
    """
    Generate queries to find zip codes with the lowest and highest values for each metric within a state.
    """
    min_queries = []
    max_queries = []
    for metric in metrics:
        min_queries.append(f"""
            SELECT MIN({metric}) AS min_{metric}, zip_code 
            FROM weather_data
            WHERE time >= '{start_time}' AND time < '{end_time}' AND "state" = '{state}'
            GROUP BY zip_code
        """)
        max_queries.append(f"""
            SELECT MAX({metric}) AS max_{metric}, zip_code 
            FROM weather_data
            WHERE time >= '{start_time}' AND time < '{end_time}' AND "state" = '{state}'
            GROUP BY zip_code
        """)
    return (min_queries, max_queries)


def get_all_states():
    """
    Retrieve a list of all states (including DC) from the database.
    """
    query = 'SHOW TAG VALUES FROM weather_data WITH KEY = "state"'
    result = client.query(query)
    states = [item["value"] for item in result.get_points()]
    return states


def calculate_extremes(min_queries, max_queries, client, metrics):
    """
    Extract the results of min and max queries for extremes calculation.
    """
    extremes = []
    try:
        # Process min queries
        for metric, query in zip(metrics, min_queries):
            result = client.query(query)
            for group_key, points in result.items():
                zip_code = group_key[1].get('zip_code')
                points_list = list(points)
                if points_list:
                    value = points_list[0].get(f"min_{metric}")
                    if value is not None:
                        extremes.append({"zip_code": zip_code, f"min_{metric}": value})

        # Process max queries
        for metric, query in zip(metrics, max_queries):
            result = client.query(query)
            for group_key, points in result.items():
                zip_code = group_key[1].get('zip_code') 
                points_list = list(points)
                if points_list:
                    value = points_list[0].get(f"max_{metric}")
                    if value is not None:
                        extremes.append({"zip_code": zip_code, f"max_{metric}": value})

    except Exception as e:
        logging.error(f"PROCESSOR: InfluxDB query failed: {e}")
    return extremes

def send_analytics_to_influxdb(hour, analytics_data, measurement):
    """
    Save analytics results (averages or extremes) to InfluxDB.
    """
    points = []
    for data in analytics_data:
        fields = {k: v for k, v in data.items() if k not in ["zip_code", "state"]}
        tags = {"hour": hour}
        if "zip_code" in data:
            tags["zip_code"] = data["zip_code"]
        if "state" in data:
            tags["state"] = data["state"]

        points.append({
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": f"2023-09-19T{hour}:00:00Z"
        })

    try:
        client.write_points(points)
        logging.info(f"PROCESSOR: Successfully wrote analytics to InfluxDB for hour {hour} into {measurement}")
    
    except Exception as e:
        logging.error(f"PROCESSOR: InfluxDB write failed: {e}")
        pending_data.put(points)

def handle_incoming_messages():
    """Listen for Ingester notifications and start processing received data."""
    while True:
        try:
            pubsub = r.pubsub()
            pubsub.psubscribe("weather_channel:processor:*")
            logging.info("PROCESSOR: Subscribed to Ingester notifications.")
            success = True
            for message in pubsub.listen():
                if message['type'] == 'pmessage':
                    channel = message['channel']
                    if channel.startswith("weather_channel:processor:"):
                        hour = int(message['data'])
                        logging.info(f"PROCESSOR: Received notification to process data for hour {hour}")
                        process_hourly_data(hour)
                
        except redis.ConnectionError:
            logging.error("PROCESSOR: Redis connection lost. Attempting to reconnect...")
            sleep(5)


channel_handler = threading.Thread(target=handle_incoming_messages, daemon=True)
channel_handler.start()

# Keep the main thread alive to maintain the background threads
try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    logging.info("STREAMER: Shutting down.")
    print("Shutting down...")

