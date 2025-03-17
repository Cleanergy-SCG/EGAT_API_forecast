# Use official Python image as base
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install necessary drivers for MS SQL Server
RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for SQL Server (modify as needed)
# ENV MSSQL_SERVER="your_server"
# ENV MSSQL_DATABASE="your_database"
# ENV MSSQL_USER="your_user"
# ENV MSSQL_PASSWORD="your_password"

# Copy the application code
COPY . .

# Create output directory with write permissions
RUN mkdir -p /app/output_directory && chmod -R 777 /app/output_directory

# Expose FastAPI default port
EXPOSE 8000

# Run FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "18901"]
