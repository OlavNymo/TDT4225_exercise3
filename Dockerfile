FROM python:3.9

# Install MySQL client
RUN apt-get update && apt-get install -y default-mysql-client

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables
ENV MYSQL_HOST=mysql
ENV MYSQL_DATABASE=geolife
ENV MYSQL_USER=root
ENV MYSQL_PASSWORD=group20

# Command to run when starting the container
CMD ["python", "main.py"]