# Use an official Python runtime as a parent image
FROM python:3

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

WORKDIR /app/src

# Install any necessary packages
RUN pip install -r requirements.txt

# Expose port 9000 to allow access to the Flask service
EXPOSE 9000

# Run Flask application
CMD ["python3", "flask_test.py"]
