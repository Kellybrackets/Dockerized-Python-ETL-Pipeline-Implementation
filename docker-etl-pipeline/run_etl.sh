#!/bin/bash

# Docker ETL Pipeline Runner
set -e

echo "==========================================="
echo "    Docker ETL Pipeline Starter"
echo "==========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Build and start containers
print_status "Building and starting Docker containers..."
docker-compose down > /dev/null 2>&1 || true
docker-compose up --build -d

# Wait for database to be ready
print_status "Waiting for database to be ready..."
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if docker exec etl_database pg_isready -U etl_user -d etl_db > /dev/null 2>&1; then
        print_status "Database is ready!"
        break
    fi
    
    if [ $attempt -eq $max_attempts ]; then
        print_error "Database failed to start within expected time"
        docker-compose logs database
        exit 1
    fi
    
    attempt=$((attempt + 1))
    sleep 2
done

# Wait a bit more to ensure database is fully initialized
sleep 5

# Run the ETL pipeline
print_status "Starting ETL pipeline..."
print_status "This may take a few moments..."

# Execute the ETL pipeline
if docker exec etl_pipeline python /app/src/main.py; then
    print_status "ETL pipeline completed successfully!"
else
    print_error "ETL pipeline failed!"
    print_status "Showing logs for debugging:"
    docker-compose logs etl_pipeline
    exit 1
fi

# Display final status
print_status "Displaying container status:"
docker-compose ps

print_status "Displaying recent logs from ETL pipeline:"
docker logs --tail=20 etl_pipeline

echo ""
echo "==========================================="
print_status "ETL Pipeline execution completed!"
echo "==========================================="
echo ""
print_status "To view logs: docker-compose logs -f"
print_status "To stop containers: docker-compose down"
print_status "To restart ETL: ./run_etl.sh"
echo ""
