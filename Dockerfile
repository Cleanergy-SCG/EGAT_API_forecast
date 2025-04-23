# Use official Python image as base
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install necessary drivers for MS SQL Server
RUN apt-get update && apt-get install -y wget apt-transport-https gnupg

RUN wget https://packages.microsoft.com/keys/microsoft.asc -O- | apt-key add -
RUN wget https://packages.microsoft.com/config/debian/11/prod.list -O /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Set timezone
ENV TZ=Asia/Bangkok

# Copy the application code
COPY . .

# Expose FastAPI default port
EXPOSE 8001

# Run FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
