# ✈️ Flight Operations Medallion Pipeline & Analytics

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-DWH-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://snowflake.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-24%2F7%20Cloud-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)

An end-to-end, production-grade **Flight Operations Data Engineering Pipeline** built following the **Medallion Architecture** (Bronze ➔ Silver ➔ Gold). The project ingests live global aircraft telemetry, cleanses and aggregates flight KPIs, loads them into **Snowflake Data Warehouse** using idempotent SQL `MERGE` operations, and displays interactive insights via a **Streamlit Web Dashboard**.

---

## 🔗 Quick Links & Live Demos

* 🌐 **Live Interactive Dashboard**: [https://flight-ops-airflow-zanr2u25c9aoxjhu37qk7s.streamlit.app/](https://flight-ops-airflow-zanr2u25c9aoxjhu37qk7s.streamlit.app/)
* 🐙 **GitHub Repository**: [https://github.com/icedoutchirag/flight-ops-airflow](https://github.com/icedoutchirag/flight-ops-airflow)

---

## 📐 System Architecture

```mermaid
flowchart LR
    subgraph Data Source
        API[OpenSky Network API\nLive Aircraft State Vectors]
    end

    subgraph Data Pipeline (Medallion Architecture)
        Bronze[🥉 Bronze Layer\nRaw JSON Files\n/data/bronze]
        Silver[🥈 Silver Layer\nCleaned CSV Telemetry\n/data/silver]
        Gold[🥇 Gold Layer\nAggregated KPIs CSV\n/data/gold]
    end

    subgraph Orchestration & Storage
        Airflow[⚙️ Apache Airflow 2.9.3\nScheduler & Webserver]
        GHA[⚡ GitHub Actions\n24/7 Cloud Automation]
        SF[(❄️ Snowflake DWH\nFLIGHT_DB.PUBLIC.FLIGHT_KPIS)]
    end

    subgraph Analytics & Visualization
        Dash[📊 Streamlit Cloud\nLive Public Web App]
    end

    API -->|HTTP GET| Bronze
    Bronze -->|Clean & Filter| Silver
    Silver -->|GroupBy Country| Gold
    Gold -->|Idempotent MERGE| SF
    Airflow -->|Local Orchestration| Bronze
    GHA -->|Cloud Orchestration| Bronze
    SF -->|SQL Queries| Dash
```

---

## 💡 How the Pipeline Works

The data pipeline processes real-time global aviation data across three distinct Medallion layers:

### 1. 🥉 Bronze Layer (Raw Ingestion)
* **Script**: [`scripts/bronze_ingest.py`](file:///d:/project%20interview/flight-ops-airflow-main/scripts/bronze_ingest.py)
* Ingests real-time flight state vectors from the **OpenSky Network REST API**.
* Automatically creates the directory structure and stores raw response payloads as timestamped JSON files (`/data/bronze/flights_YYYYMMDDHHMMSS.json`).
* Passes the generated filepath to downstream tasks via Airflow XCom.

### 2. 🥈 Silver Layer (Cleaning & Standardization)
* **Script**: [`scripts/silver_transform.py`](file:///d:/project%20interview/flight-ops-airflow-main/scripts/silver_transform.py)
* Reads raw JSON files from the Bronze layer.
* Maps column names (`icao24`, `callsign`, `origin_country`, `velocity`, `on_ground`, etc.).
* Filters out extraneous fields and standardizes clean tabular data saved to `/data/silver/flights_silver_<date>.csv`.

### 3. 🥇 Gold Layer (Business Aggregation)
* **Script**: [`scripts/gold_aggregate.py`](file:///d:/project%20interview/flight-ops-airflow-main/scripts/gold_aggregate.py)
* Aggregates flight telemetry grouped by `origin_country`.
* Computes key metrics:
  * `TOTAL_FLIGHTS`: Count of active aircraft per country.
  * `AVG_VELOCITY`: Mean velocity of airborne aircraft.
  * `ON_GROUND`: Count of aircraft currently on the ground.
* Exports summary data to `/data/gold/flights_gold_<date>.csv`.

### 4. ❄️ Data Warehouse Loading (Snowflake Integration)
* **Script**: [`scripts/load_gold_to_snowflake.py`](file:///d:/project%20interview/flight-ops-airflow-main/scripts/load_gold_to_snowflake.py)
* Uses `snowflake-connector-python` to connect to Snowflake.
* Executes idempotent **`MERGE INTO FLIGHT_KPIS`** statements. If a record for the current execution window and origin country already exists, it updates the metrics; otherwise, it inserts a new record.

---

## 🛠️ Key Technical & Engineering Highlights

* **Idempotency**: Pipeline runs are completely idempotent. Re-running the pipeline for any execution window updates existing Snowflake records cleanly without creating duplicate rows.
* **Dual Execution Model**:
  * **Local Development**: Docker Compose running Airflow 2.9.3 + PostgreSQL 15.
  * **24/7 Cloud Automation**: Automated **GitHub Actions runner** that executes every 30 minutes in the cloud for $0 cost.
* **Credential Fallback**: `load_gold_to_snowflake.py` dynamically handles authentication via **Airflow BaseHook** connections in local mode OR **Environment Variables / GitHub Secrets** in cloud mode.

---

## 🗂️ Project Directory Structure

```text
flight-ops-airflow/
├── .github/
│   └── workflows/
│       └── pipeline.yml          # 24/7 Automated Cloud Runner (GitHub Actions)
├── dags/
│   └── flight-pipeline.py        # Airflow DAG definition (*/30 schedule)
├── scripts/
│   ├── bronze_ingest.py          # Phase 1: OpenSky API Ingestion
│   ├── silver_transform.py        # Phase 2: Schema Parsing & Cleaning
│   ├── gold_aggregate.py         # Phase 3: Country KPI Aggregations
│   └── load_gold_to_snowflake.py # Phase 4: Idempotent Snowflake Upsert
├── .env                          # Local Environment Variables
├── .gitignore                    # Git exclusions
├── dashboard.py                  # Streamlit Web Application
├── docker-compose.yml            # Airflow + Postgres Docker multi-container setup
├── requirements.txt              # Python Dependencies
└── README.md                     # Documentation
```

---

## ⚙️ How to Run Locally

### Prerequisites
* **Docker Desktop** installed and running.
* **Git** installed.

### Step 1: Clone Repository
```bash
git clone https://github.com/icedoutchirag/flight-ops-airflow.git
cd flight-ops-airflow
```

### Step 2: Start Containers with Docker Compose
```bash
docker compose up -d
```
*Wait ~30 seconds for `airflow-init` container to initialize the PostgreSQL database and create the `admin` user.*

### Step 3: Access Airflow Web UI
Open your browser and navigate to: **`http://localhost:8080`**
* **Username**: `admin`
* **Password**: `admin`

### Step 4: Add Snowflake Connection in Airflow
1. In Airflow UI, navigate to **Admin ➔ Connections ➔ Add Record (+)**.
2. Fill in connection parameters:
   * **Connection Id**: `flight_snowflake`
   * **Connection Type**: `Generic`
   * **Schema**: `PUBLIC`
   * **Login**: `<your_snowflake_username>`
   * **Password**: `<your_snowflake_password>`
   * **Extra**:
     ```json
     {
       "account": "YOUR_ACCOUNT_LOCATOR",
       "warehouse": "COMPUTE_WH",
       "database": "FLIGHT_DB",
       "role": "ACCOUNTADMIN"
     }
     ```

### Step 5: Trigger the DAG
1. On the **DAGs** tab, turn `flights_ops_medallion_pipe` to **ON**.
2. Click **Trigger DAG** (▶ button) to run the medallion pipeline.

### Step 6: Launch Streamlit Dashboard Locally
```bash
pip install -r requirements.txt
streamlit run dashboard.py
```
Open **`http://localhost:8501`** in your browser.

---

## 🛢️ Snowflake DDL Reference

To set up the target table in your Snowflake instance manually:

```sql
-- 1. Create Database and Schema
CREATE DATABASE IF NOT EXISTS FLIGHT_DB;
CREATE SCHEMA IF NOT EXISTS FLIGHT_DB.PUBLIC;

-- 2. Create Target KPI Table
CREATE TABLE IF NOT EXISTS FLIGHT_DB.PUBLIC.FLIGHT_KPIS (
    WINDOW_START TIMESTAMP_NTZ,
    ORIGIN_COUNTRY VARCHAR(100),
    TOTAL_FLIGHTS INT,
    AVG_VELOCITY FLOAT,
    ON_GROUND INT,
    LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Query Data
SELECT * FROM FLIGHT_DB.PUBLIC.FLIGHT_KPIS ORDER BY TOTAL_FLIGHTS DESC;
```

---

## 🌐 24/7 Cloud Architecture

```text
[GitHub Actions Cron: */30] ──> [Medallion Python Pipeline] ──> [Snowflake DWH]
                                                                        │
[Streamlit Cloud App] <─────────────────────────────────────────────────┘
```

The cloud deployment is 100% serverless and costs **$0.00/month**:
1. **GitHub Actions** runs `.github/workflows/pipeline.yml` every 30 minutes.
2. The workflow executes all 4 medallion steps and upserts data into **Snowflake**.
3. **Streamlit Community Cloud** renders the live interactive frontend querying Snowflake directly.

---

## 👨‍💻 Author

Developed by **Chirag** ([@icedoutchirag](https://github.com/icedoutchirag)).  
*Built for Data Engineering Portfolio & Demonstration.*
