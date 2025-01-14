#!/bin/bash





echo "Initializing Airflow database..."

# Run Airflow database initialization
if docker-compose up airflow-init; then
    echo "Airflow database initialized successfully."
else
    echo "Error: Failed to initialize the Airflow database. Check the logs for details."
    exit 1
fi

echo "Starting Docker containers in detached mode..."

# Start all containers in detached mode 
if docker-compose up -d ; then
    echo "Docker containers started successfully."
else
    echo "Error: Failed to start Docker containers. Check the logs for details."
    exit 1
fi

# Wait for containers to stabilize
sleep 5

# Checking  the status of the containers
echo "Checking container status..."
docker ps


echo "Docker  setup is complete!"
