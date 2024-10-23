# Dockerfile
FROM python:3.9

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Copy the rest of the application code
COPY . .

# Set environment variables
ENV MONGODB_URI=mongodb://mongodb:27017/
ENV MONGODB_DATABASE=geolife

# Command to run when starting the container
CMD ["python", "main.py"]
