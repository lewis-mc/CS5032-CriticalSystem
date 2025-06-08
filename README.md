# CS5032-CriticalSystem
In this practical the task is to create an interactive weather station critical system that will replicate the process of real-time, live weather information reporting. Within Python Redis and InfluxDB will be utilised to complete this task. Redis will communicate between each of the different components: Streamer, Ingester and Processor. 

# How to Run

First within the src/ directory run:
    pip install -r requirements.txt to make sure all dependencies are available

To create the Redis and InfluxDB containers in the /P1 directory execute:
    podman-compose up

To start the whole process, in the /src directory execute:
    ./start.sh

    This will:
    - Reset the database to make sure it is empty
    - Start the process of running the Streamer, Ingester, Processor and Flask instance


- Redis: 
    - host: 127.0.0.1
    - port: 6379
- InfluxDB:
    - host: 127.0.0.1
    - port: 8086
- Flask:
    - host: 127.0.0.1
    - port: 9000
    Flask API can be accessed at http://127.0.0.1:9000

