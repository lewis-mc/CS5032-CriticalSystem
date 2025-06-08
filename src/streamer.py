import redis
import time
import pandas as pd
import logging
import json
import os
import threading
import queue
from time import sleep

# Initialize logging
logging.basicConfig(filename="logs.csv", level=logging.INFO, format="%(asctime)s, %(message)s")

# Initialize the Redis instance
r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

# Load data and add hour column for processing
df = pd.read_csv("../data/weather_data.csv")
df['hour'] = pd.to_datetime(df['time']).dt.hour
batch_size = 500
backup_dir = "backup_data"
pending_data = queue.Queue()  


def get_data_hour(hour):
    """Retrieve data for the specified hour."""
    return df[df['hour'] == hour]

def publish_data(channel, message):
    """Attempt to publish data to Redis. If Redis is unavailable, queue data."""
    try:
        r.publish(channel, message)
    
    except redis.ConnectionError:
        logging.error(f"STREAMER: Redis down, queuing data for {channel}")
        pending_data.put((channel, message))  # Add to queue if Redis is down
        sleep(0.1)

# Threaded function to publish data every 30 seconds
def publish_data_thread():
    while True:
        for hour in range(24):
            logging.info(f"STREAMER: Starting to publish data for hour {hour}")
            data_hour = get_data_hour(hour)
            
            # Publish data in batches
            for batch_index, batch_offset in enumerate(range(0, len(data_hour), batch_size)):
                batch = data_hour.iloc[batch_offset:batch_offset + batch_size]
                batch_json = batch.to_json(orient='records')
                # Publish data
                if batch_offset + batch_size >= len(data_hour): # Last batch
                    publish_data(f"weather_channel:data:LAST:{hour}", batch_json)
                    logging.info(f"STREAMER: Publishing batch index LAST for hour {hour}")
                else:
                    publish_data(f"weather_channel:data:{batch_index}:{hour}", batch_json)
            # Sleep for 5 minutes before publishing the next hour's data
            sleep(600)

def pending_thread_handler():
    while True:
        send_pending_data()
        sleep(30)

# Thread for listening to replay requests
def send_pending_data():
    """Send pending data to Redis."""
    
    if pending_data.empty():
        logging.info("STREAMER: No pending data to send.")
        return
    try:
        while not pending_data.empty():
            channel, message = pending_data.get()
            r.publish(channel, message)
            logging.info("STREAMER: Sent pending data to Redis.")
            sleep(0.1)

    except redis.ConnectionError:
        logging.error(f"STREAMER: Redis still down, requeuing data for {channel}")
        pending_data.put((channel, message))  # Add to queue if Redis is down
        sleep(15) # timeout 
            

def get_parts(channel) :
    channel_name, type_message, batch_index, hour = channel.split(":")
    return channel_name, type_message, batch_index, hour

def get_data_hour_batch(batch_index, hour):
    data_hour = get_data_hour(int(hour))
    batch_offset = int(batch_index) * batch_size
    batch = data_hour.iloc[batch_offset:batch_offset + batch_size]
    return batch.to_json(orient='records')

def listening_incoming_messages():
    """Listen for replay requests from ingester and republish data if requested."""
    
    while True:
        try: 
            pubsub = r.pubsub()
            pubsub.psubscribe("weather_channel:request:*")

            for message in pubsub.listen():

                if message["type"] == "pmessage":
                    channel = message["channel"]
                    channel_name, type_message, batch_index, hour = get_parts(channel)
                
                    if channel.startswith("weather_channel:request:"):
                        logging.info(f"STREAMER: Received request for data:{batch_index}:{hour}")
                        batch_data = get_data_hour_batch(batch_index, hour)
                        publish_data(f"weather_channel:data:{batch_index}:{hour}", batch_data)
        except redis.ConnectionError:
            sleep(15)
                

# Start the data publishing thread
publish_thread = threading.Thread(target=publish_data_thread, daemon=True)
publish_thread.start()

# Start listening for replay requests in a separate thread
pending_data_thread = threading.Thread(target=pending_thread_handler, daemon=True)
pending_data_thread.start()

# Start listening for replay requests in a separate thread
listening_thread = threading.Thread(target=listening_incoming_messages, daemon=True)
listening_thread.start()

# Keep the main thread alive to maintain the background threads
try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    logging.info("STREAMER: Shutting down.")
    print("Shutting down...")
