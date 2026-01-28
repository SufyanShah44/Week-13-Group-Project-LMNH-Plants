# LMNH Plants Data Pipelines

This repository contains the data pipelines for the **LMNH Plants project**.

The system consists of **two deployed pipelines**:

1. **Short-term ingestion pipeline**
   - Pulls raw plant and sensor data from the API
   - Stores short-term operational data in an RDS database
   - Data is **automatically deleted every 24 hours**

2. **Long-term aggregation pipeline**
   - Reads short-term data
   - Produces aggregated and summarised datasets
   - Stores results in **S3** for analytics and dashboards
   - Data in S3 is persistent and queryable

---

## High-level architecture

API  
→ Short-term pipeline  
→ RDS (24 hour retention)  
→ Aggregation pipeline  
→ S3 (long-term analytics)  
→ Dashboard / Athena

---

## Pipeline responsibilities

### Short-term ingestion pipeline (RDS)

- Runs on the cloud (scheduled or event based)
- Writes raw operational data to RDS
- Enforces relational integrity
- Data is disposable and cleared every 24 hours
- Used only as an intermediate processing layer

### Long-term aggregation pipeline (S3)

- Reads data from RDS
- Produces aggregated and dashboard ready datasets
- Writes outputs to S3
- Data is persistent and used by dashboards
- Decouples analytics from operational workloads

---

## Repository structure

Typical structure:

.
├── pipeline/
│   ├── extract_api_to_csv.py
│   ├── load_short_term.py
│   └── aggregate_to_s3.py
├── output.csv
├── requirements.txt
├── .env.example
└── README.md

---

## Prerequisites

- Python 3.10+
- ODBC Driver for SQL Server
- Access to the RDS database
- Access to the S3 bucket

---

## Local setup

### Create a virtual environment

python -m venv .venv  
source .venv/bin/activate

### Install dependencies

pip install -r requirements.txt

Minimum required packages:

pandas  
python-dotenv  
pyodbc  
boto3

---

## Environment variables

Create a `.env` file in the project root.

Example:

DB_DRIVER=ODBC Driver 18 for SQL Server  
DB_HOST=YOUR_DB_HOST  
DB_PORT=1433  
DB_NAME=plants  
DB_USERNAME=alpha  
DB_PASSWORD=YOUR_PASSWORD  

AWS_REGION=eu-west-2  
S3_BUCKET_NAME=lmnh-plants-analytics

---

## Database assumptions (short-term pipeline)

Tables expected:

alpha.countries  
- country_id (PK)  
- country_name  

alpha.origin_location  
- origin_location_id (PK)  
- city  
- country_id (FK)  
- latitude  
- longitude  

alpha.plants  
- plant_id (PK)  
- name  
- scientific_name  
- origin_location_id (FK)

---

## Running the pipelines locally

### Step 1: Extract API data

python pipeline/extract_api_to_csv.py

Creates or updates output.csv

---

### Step 2: Load short-term data into RDS

python pipeline/load_short_term.py

This step:
- maps country text to country_id
- ensures origin locations exist
- resolves origin_location_id
- inserts only new plants

Safe to re run.

---

### Step 3: Run aggregation pipeline

python pipeline/aggregate_to_s3.py

This step:
- queries RDS
- generates aggregated datasets
- uploads results to S3

---

## Data retention policy

- RDS holds short-term data only (24 hour retention)
- S3 holds long-term analytical data
- RDS cleanups do not affect S3 outputs

---

