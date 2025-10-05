# Dockerized Python ETL Pipeline

A complete Dockerized ETL (Extract, Transform, Load) pipeline demonstrating data processing with Python and PostgreSQL.

## Project Overview

This project implements a fully containerized ETL pipeline that:
- Extracts data from a PostgreSQL database and simulated external sources
- Transforms data with business logic (cleaning, calculations, categorizations)
- Loads processed data back into the database
- Runs entirely in Docker containers with proper networking

## Architecture

- **Database Container**: PostgreSQL with initial schema and sample data
- **ETL Container**: Python application with extraction, transformation, and loading logic
- **Docker Network**: Isolated network for container communication
- **Bash Script**: Automated setup and execution

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Git

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd docker-etl-pipeline
chmod +x run_etl.sh
```

### 2. Run the Pipeline

```bash
./run_etl.sh
```

## This script will:

- Build Docker images
- Start containers in detached mode
- Wait for database initialization
- Execute the ETL pipeline
- Display results and logs

### 3. Manual Execution (Alternative)

If you prefer to run steps manually:

```bash
# Build and start containers
docker-compose up --build -d

# Wait for database to be ready, then run ETL
docker exec etl_pipeline python /app/src/main.py

# View logs
docker-compose logs -f

# Stop containers
docker-compose down
```
### Project Structure
```text
docker-etl-pipeline/
├── docker-compose.yml          # Multi-container configuration
├── run_etl.sh                  # Automated execution script
├── database/
│   ├── Dockerfile             # Database container definition
│   └── init.sql               # Database schema and sample data
├── etl/
│   ├── Dockerfile             # ETL application container definition
│   ├── requirements.txt       # Python dependencies
│   └── src/
│       ├── extract.py         # Data extraction logic
│       ├── transform.py       # Data transformation logic
│       ├── load.py           # Data loading logic
│       └── main.py           # ETL orchestration
└── README.md
```
## ETL Process Details
### Extraction Phase
-Extracts raw data from PostgreSQL database
-Simulates external data source (API/CSV)
-Combines multiple data sources
-Handles connection errors and data validation

### Transformation Phase
-Data cleaning and deduplication
-Business logic application:
-Tax calculation (8%)
-Transaction size categorization
-Customer segmentation
-Date formatting
-Data validation and error handling

### Loading Phase
-Updates original records with processed data
-Inserts into dedicated processed data table
-Handles duplicates with upsert logic
-Provides data verification and summary

## Database Schema
-Source Table **(sales_data)**
-Raw transaction data
-Includes customer, product, and transaction details
- Includes calculated fields and categorizations

License
This project is for educational purposes as part of a Docker assignment.

text

## Usage Instructions

1. **Make the script executable:**
   ```bash
   chmod +x run_etl.sh
   ```
2. Run the complete pipeline:

```bash
./run_etl.sh
```
3. **The script will:**

- Build Docker images

- Start the database and ETL containers

- Wait for database initialization

- Execute the full ETL pipeline

- Display results and verification

4. ** To stop and clean up: **

```bash
docker-compose down
```
