import redis
import json
import logging
from time import sleep
from influxdb import InfluxDBClient
import os
import queue
import threading
from influxdb.exceptions import InfluxDBClientError
from requests.exceptions import ConnectionError

# Initialize logging
logging.basicConfig(filename="logs.csv", level=logging.INFO, format="%(asctime)s, %(message)s")

# Initialize Redis and InfluxDB clients
r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'myDB')
pending_messages = queue.Queue()  # Queue to store messages when Redis is down

cached_data = queue.Queue()  # Queue to store data
received_batches_indexes = {}  # Store received batch indexes for each hour
num_batches_per_hour = 88 # Number of batches per hour

influx_online = True

def handle_message(message):
    """Process each message received from Redis."""
    try:
        batch_data = json.loads(message['data'])
        send_raw_data_to_influxdb(batch_data)
        

    except redis.ConnectionError as e: # is this needed
        logging.error(f"INGESTER: Redis down, unable to process message: {e}")

    except json.JSONDecodeError as e:
        logging.error(f"INGESTER: Invalid JSON data, unable to process message: {e}")


def send_raw_data_to_influxdb(batch_data):
    global influx_online

    """Save raw weather data to the 'weather_data' measurement in InfluxDB."""
    points = []
    for entry in batch_data:
        points.append({
            "measurement": "weather_data",
            "tags": {
                "zip_code": entry["zip_code"],
                "state": entry["state"]
            },
            "fields": {
                "temp_c": entry["temp_c"],
                "pressure_mb": entry["pressure_mb"],
                "humidity": entry["humidity"],
                "precip_mm": entry["precip_mm"]
            },
            "time": entry["time"]
        })

    try:
        client.write_points(points)
    
    except ConnectionError as e:
        logging.error(f"INGESTER: Connection error, queuing data: {e}")
        influx_online = False
        sleep(5) # Wait for Redis to come back online
    
    influx_online = True

def notify_processor(hour):
    """Notify Processor to begin analytics for a completed hour."""
    try:
        r.publish(f"weather_channel:processor:{hour}", hour)
        logging.info(f"INGESTER: Notified Processor to start analytics for hour {hour}")
    except redis.ConnectionError:
        logging.error(f"INGESTER: Failed to notify Processor for hour {hour}")
        pending_messages.put(f"weather_channel:processor:{hour}", hour)

def send_pending():
    # send any data that is cached
    global cached_data
    for message in list(cached_data.queue):
        handle_message(message)
    for message in list(pending_messages.queue):
        print("pending messages")
        pending_messages.queue.remove(message)

    

def pending_data_thread_handler():
    while True:
        send_pending()
        sleep(15)

def get_parts(channel) :
    parts = channel.split(":")
    channel_name = parts[0]
    type_message = parts[1]
    batch_index = parts[2]
    hour = parts[3]
    return channel_name, type_message, batch_index, hour

def clear_cached_data_for_hour(hour):
    """Remove cached messages for a specific hour after all batches are received."""
    global cached_data
    cached_data.queue = [msg for msg in cached_data.queue if get_parts(msg['channel'])[3] != hour]

def all_batches_received(hour):
    """Check if all batches for the hour have been received."""

    global received_batches_indexes
    global cached_data

    cached_data_list = list(cached_data.queue)

    for cached_data_item in cached_data_list:

        message = cached_data_item
        channel = message['channel']
        channel_name, type_message, batch_index, message_hour = get_parts(channel)
        if hour not in received_batches_indexes:
                received_batches_indexes[hour] = set()
        # Check if the message is for the same hour
        if message_hour == hour:
            # Check if all batches are present for the hour
            if batch_index == "LAST":
                received_batches_indexes[hour].add(num_batches_per_hour-1)
            else:
                received_batches_indexes[hour].add(int(batch_index))
    
    for index in range(num_batches_per_hour):
        if index not in received_batches_indexes[hour]:    
            return False
    
    return True
    

def request_batches(hour):
    """Request any missing batches for the hour."""

    global received_batches_indexes
    global cached_data

    missing_batches = set()
    for index in range(num_batches_per_hour-1):
        if index not in received_batches_indexes[hour]:
            missing_batches.add(index)
    try:
        # iterate through all missing batches and request them
        for batch_index in missing_batches:
            r.publish(f"weather_channel:request:{batch_index}:{hour}", hour)
            logging.info(f"INGESTER: Requested missing batch {batch_index} for hour {hour}")
            sleep(0.1)
            return batch_index

    except redis.ConnectionError:
        logging.error(f"INGESTER: Failed to request missing batches for hour {hour}")
        sleep(10)

def send_all_cached_data(hour):
    """Send all cached data for the hour to InfluxDB."""
    global cached_data
    logging.info(f"INGESTER: Sending all cached data for hour {hour}")
    for message in list(cached_data.queue):
        if get_parts(message['channel'])[3] == hour:
            handle_message(message)

def handle_incoming_messages():
    global cached_data
    
    while True:

        try:
            pubsub = r.pubsub()
            pubsub.psubscribe("weather_channel:data:*")
            logging.info("INGESTER: Subscribed to Streamer data channels.")
            received_last = False
            last_request_index = -1

            for message in pubsub.listen():

                if message['type'] == 'pmessage':
                    
                    channel = message['channel']
                    channel_name, type_message, batch_index, hour = get_parts(channel)

                    if channel.startswith("weather_channel:data:"):
                        if batch_index == "LAST": # last batch, make sure all other batches have been received
                            received_last = True
                        
                        cached_data.put(message)
            
                        if all_batches_received(hour):
                            send_all_cached_data(hour)
                            notify_processor(hour)
                            clear_cached_data_for_hour(hour)
                            # logging.info(f"INGESTER: Completed receiving data for hour {hour}")
                            received_last = False
                        elif received_last:
                            if last_request_index == request_batches(hour):
                                logging.info(f"INGESTER: Requested the same batch {last_request_index} for hour {hour}")
                                sleep(15)
                            


        except redis.ConnectionError:
            logging.error("INGESTER: Redis connection lost. Attempting to reconnect...")
            sleep(5)
        except InfluxDBClientError:
            logging.error("INGESTER: InfluxDB connection lost. Attempting to reconnect...")
            sleep(5)

# Start the data publishing thread
channel_handler = threading.Thread(target=handle_incoming_messages, daemon=True)
channel_handler.start()

pending_data_thread = threading.Thread(target=pending_data_thread_handler, daemon=True)
pending_data_thread.start()

# Keep the main thread alive to maintain the background threads
try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    logging.info("STREAMER: Shutting down.")
    print("Shutting down...")
