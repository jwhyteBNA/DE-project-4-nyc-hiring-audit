# NYC Hiring Audit

This project ingests, processes, and analyzes NYC job posting and payroll data using Python, MinIO, Snowflake, Airflow, and dbt.  
It is designed for medallion architecture (bronze/silver/gold) ETL workflows in a reproducible devcontainer environment.

---

## Features

- **Data Ingestion:**  
  - Pulls raw job and payroll data from APIs and files.
  - Streams large datasets in batches for memory efficiency.
  - Stores raw data in MinIO (S3-compatible object storage).

- **Data Staging:**  
  - Loads data from MinIO into Snowflake bronze tables.
  - Handles both small and large files, with batch processing for scalability.

- **Data Transformation:**  
  - Uses dbt to clean, document, and test data in Snowflake silver/gold layers.

- **Orchestration:**  
  - Airflow DAGs automate ingestion and staging workflows.

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) (for devcontainer)
- [VS Code](https://code.visualstudio.com/) with Remote - Containers extension
- Snowflake account (trial or paid)
- MinIO server (local or cloud)

### Setup

1. **Clone the repository:**
   ```sh
   git clone https://github.com/your-org/project-4-nyc-hiring-audit.git
   cd project-4-nyc-hiring-audit
   ```

2. **Open in VS Code and start the devcontainer.**

3. **Configure environment variables:**
   - Create `.env` and fill in your credentials for MinIO and Snowflake.

4. **Install Python dependencies:**
   ```sh
   pip3 install -r requirements.txt
   ```

5. **Set up MinIO and Snowflake:**
   - Ensure your MinIO bucket is accessible and S3-compatible.
   - Create necessary Snowflake databases, schemas, and stages.

---

## Usage

### Data Ingestion

- Run the ingestion script to pull and stage data:
  ```sh
  python3 src/ingest.py
  python3 src/connection.py
  ```

### Airflow Orchestration

- Start Airflow and trigger DAGs for automated workflows:
  ```sh
  airflow webserver
  airflow scheduler
  ```

### dbt Transformation

- Run dbt models and tests:
  ```sh
  cd nyc_audit
  dbt run
  dbt test
  ```

---

## Project Structure

```
├── src/
│   ├── ingest.py         # API/file ingestion to MinIO
│   ├── connection.py     # MinIO to Snowflake ETL
│   └── logs/             # Log files
├── nyc_audit/
│   ├── models/           # dbt models (SQL)
│   ├── seeds/            # dbt seed data
│   └── schema.yml        # dbt schema and tests
├── airflow/
│   └── dags/             # Airflow DAGs
├── requirements.txt
├── .env
└── README.md
```


## Contact

For questions or support, open an issue or contact the maintainers.