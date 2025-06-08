from flask import Flask, request, jsonify
from influxdb import InfluxDBClient

# Initialize InfluxDB client
client = InfluxDBClient('localhost', 8086, 'root', 'root', 'myDB')
app = Flask(__name__)

@app.route('/')
def respond():
    return 'WeatherVane API is Online!'

@app.route('/avg', methods=['GET'])
def get_average():
    """
    Fetch average temperature, pressure, humidity, and precipitation 
    for a specific state and hour from the `weather_averages` measurement.
    """
    state = request.args.get('state')
    hour = request.args.get('hour')  # Hour in "HH" format (e.g., "00", "01")

    # Validate input parameters
    if not state or not hour:
        return jsonify({"error": "State and hour parameters are required"}), 400

    # Validate hour format
    if not hour.isdigit() or int(hour) < 0 or int(hour) > 23:
        return jsonify({"error": "Invalid hour format. Use 'HH' (e.g., '00', '01')"}), 400

    # Query InfluxDB for the averages
    query = f"""
    SELECT "avg_humidity", "avg_precip_mm", "avg_pressure_mb", "avg_temp_c", "hour", "state"
    FROM "weather_averages"
    WHERE "state" = '{state}' AND "hour" = '{hour}'
    """
    try:
        result = client.query(query)
        points = list(result.get_points())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Return the results or handle no data found
    if points:
        return jsonify(points[0])
    else:
        return jsonify({"error": "No data found"}), 404
    



@app.route('/extremes', methods=['GET'])
def get_location_extremes():
    # Get parameters from the request
    state = request.args.get('state')
    hour = request.args.get('hour')

    if not state or not hour:
        return jsonify({"error": "State and hour parameters are required"}), 400

    try:
        hour = str(hour).zfill(2)  # Ensure hour is always in "HH" format (e.g., "07")
    except ValueError:
        return jsonify({"error": "Hour must be a valid integer between 0 and 23"}), 400

    metrics = ["temp_c", "pressure_mb", "humidity", "precip_mm"]
    extreme_results = {"max": {}, "min": {}}

    # Step 1: Query InfluxDB to get data for the given state and hour
    try:
        query = f"""
        SELECT *
        FROM "zip_code_extremes"
        WHERE "state" = '{state}' AND "hour" = '{hour}'
        """
        result = client.query(query)
        data_points = list(result.get_points())

        if not data_points:
            return jsonify({"error": f"No data found for state '{state}' and hour '{hour}'"}), 404

        # Step 2: Calculate max and min for each metric from the retrieved data
        for metric in metrics:
            max_metric_key = f"max_{metric}"
            min_metric_key = f"min_{metric}"

            # Initialize variables for max and min tracking
            max_value = float('-inf')
            min_value = float('inf')
            max_zip_code = None
            min_zip_code = None

            # Iterate through each record to find max and min values and their corresponding zip codes
            for record in data_points:
                if max_metric_key in record and record[max_metric_key] > max_value:
                    max_value = record[max_metric_key]
                    max_zip_code = record["zip_code"]

                if min_metric_key in record and record[min_metric_key] < min_value:
                    min_value = record[min_metric_key]
                    min_zip_code = record["zip_code"]

            # Store the max and min values along with their zip codes
            if max_value != float('-inf'):
                extreme_results["max"][metric] = {"zip_code": max_zip_code, "value": max_value}
            else:
                extreme_results["max"][metric] = None

            if min_value != float('inf'):
                extreme_results["min"][metric] = {"zip_code": min_zip_code, "value": min_value}
            else:
                extreme_results["min"][metric] = None

        # Step 3: Return the results
        return jsonify(extreme_results)

    except Exception as e:
        print(f"Error occurred: {str(e)}")  # Debugging
        return jsonify({"error": str(e)}), 500



@app.route('/state_extremes', methods=['GET'])
def get_state_extremes():
    # Get parameter from the request
    hour = request.args.get('hour')

    if not hour:
        return jsonify({"error": "Hour parameter is required"}), 400

    try:
        hour = str(hour).zfill(2)  # Ensure hour is always in "HH" format (e.g., "07")
    except ValueError:
        return jsonify({"error": "Hour must be a valid integer between 0 and 23"}), 400

    metrics = ["temp_c", "pressure_mb", "humidity", "precip_mm"]
    extreme_results = {"max": {}, "min": {}}

    # Step 1: Query InfluxDB to get data for all states at the given hour
    try:
        query = f"""
        SELECT *
        FROM "zip_code_extremes"
        WHERE "hour" = '{hour}'
        """
        result = client.query(query)
        data_points = list(result.get_points())
        print(f"Executing query: {query}")  # Debugging
        result = client.query(query)
        data_points = list(result.get_points())
        print(data_points)

        if not data_points:
            return jsonify({"error": f"No data found for hour '{hour}'"}), 404

        # Step 2: Calculate max and min for each metric from the retrieved data
        for metric in metrics:
            avg_metric_key = f"avg_{metric}"

            # Initialize variables for max and min tracking
            max_value = float('-inf')
            min_value = float('inf')
            max_state = None
            min_state = None

            # Iterate through each record to find max and min values and their corresponding states
            for record in data_points:
                state = record["state"]

                if avg_metric_key in record:
                    value = record[avg_metric_key]

                    # Update max value and corresponding state
                    if value > max_value:
                        max_value = value
                        max_state = state

                    # Update min value and corresponding state
                    if value < min_value:
                        min_value = value
                        min_state = state

            # Store the max and min values along with their states
            if max_value != float('-inf'):
                extreme_results["max"][metric] = {"state": max_state, "value": max_value}
            else:
                extreme_results["max"][metric] = None

            if min_value != float('inf'):
                extreme_results["min"][metric] = {"state": min_state, "value": min_value}
            else:
                extreme_results["min"][metric] = None

        # Step 3: Return the results
        return jsonify(extreme_results)

    except Exception as e:
        print(f"Error occurred: {str(e)}")  # Debugging
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000, debug=True)
