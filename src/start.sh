#!/bin/bash

# Function to kill each process individually
cleanup() {
    echo "Exiting. Terminating all background processes..."
    kill "$INGESTER_PID" "$PROCESSOR_PID" "$STREAMER_PID" "$FLASK_TEST_PID" 2>/dev/null
}

# Set up trap to call cleanup on EXIT or when interrupted (e.g., Ctrl+C)
trap cleanup EXIT SIGINT

# Start each Python script in the background and store its PID

python3 database.py &

python3 ingester.py &
INGESTER_PID=$!

python3 processor.py &
PROCESSOR_PID=$!

python3 streamer.py &
STREAMER_PID=$!

python3 flask_test.py &
FLASK_TEST_PID=$!

# Wait for all background processes to complete
wait
