# Use the official Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# # Set the environment variable
# ENV PYTHONUNBUFFERED=0

# Install the dependencies
RUN pip install flask

# Copy the Flask application code
COPY . .

# Expose the port on which the Flask app will run
EXPOSE 5001

# Run the Flask application
CMD ["python", "server.py"]




# CMD["python3","-m","flask","run","--host=0.0.0.0"]