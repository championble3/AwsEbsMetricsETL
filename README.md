# AWS EBS Metrics ETL Pipeline

This is an end-to-end data pipeline for extracting **EBS (Elastic Block Store) metrics** from AWS, applying necessary transformations, and loading the final data into a **Snowflake** data warehouse.

## 🚀 Pipeline Overview

The ETL process consists of the following steps:

1. **Extract**  
   - Connects to AWS CloudWatch.
   - Retrieves EBS-related metrics (e.g., VolumeReadOps, VolumeWriteOps).

2. **Transform**  
   - Cleans and reshapes the raw metrics.
   - Applies time aggregations and tagging if necessary.

3. **Load**  
   - Uploads transformed data into a **Snowflake** table.

---

## ⚙️ Technologies Used

- **Apache Airflow** – Orchestrates ETL tasks.
- **AWS Boto3 SDK** – Interacts with AWS services.
- **Snowflake Connector for Python** – Loads data into Snowflake.
- **Docker / Docker Compose** – Containerized development environment.
- **Pandas / Python** – Data transformation.

---

## 🐳 How to Run Locally (via Docker)

1. Build and start services:

   ```bash
   docker-compose up --build
