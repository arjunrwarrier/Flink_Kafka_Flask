# Use the official Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install the dependencies
RUN pip install flask

# Copy the Flask application code
COPY . .

# Expose the port on which the Flask app will run
EXPOSE 5003

# Run the Flask application
CMD ["python", "server3.py"]