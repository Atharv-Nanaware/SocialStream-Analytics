#!/bin/bash

echo "Stopping all running containers..."
docker compose down

echo "Removing all stopped containers..."
docker container prune -f


echo "Removing all volumes..."
read -p "Are you sure you want to remove all Docker volumes? This will delete data (y/N): " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
    docker volume rm $(docker volume ls -q)
    echo "All volumes removed."
else
    echo "Volume removal skipped."
fi

echo "Removing all build cache..."
docker builder prune -f


echo "Removing Airflow logs..."
if [ -d "logs" ]; then
    rm -rf logs/*
    echo "Logs removed."
else
    echo "No logs directory found."
fi

echo "Cleanup completed successfully."
