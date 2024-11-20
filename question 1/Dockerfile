# Use Python 3.9 Slim as the base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the port (if necessary)
EXPOSE 5000

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Command to run when starting the container
CMD ["python", "node.py"]
