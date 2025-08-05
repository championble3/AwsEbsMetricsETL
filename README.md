# AWS EBS Metrics ETL Pipeline

This is an end-to-end data pipeline for extracting **EBS (Elastic Block Store) metrics** from AWS, applying necessary transformations, and loading the final data into a **Snowflake** data warehouse.

## Pipeline Overview

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

## Technologies Used

- **Apache Airflow** – Orchestrates ETL tasks.
- **AWS Boto3 SDK** – Interacts with AWS services.
- **Snowflake Connector for Python** – Loads data into Snowflake.
- **Docker / Docker Compose** – Containerized development environment.
- **Pandas / Python** – Data transformation.

---

## 🛠️ Setup Instructions

### 1. Install Prerequisites

Make sure you have installed:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- Git (optional but useful)

---

### 2. Clone the Repository

```bash
git clone https://github.com/championble3/AwsEbsMetricsETL.git
cd AwsEbsMetricsETL
```

Then Create a profile in airflow 
```bash
mkdir -p airflow/dags airflow/logs airflow/plugins
```
Create an .env file
```dote
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
```


## How to Run Locally (via Docker)

1. Build and start services:

   ```bash
   docker-compose up --build





