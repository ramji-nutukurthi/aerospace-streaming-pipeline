# Aerospace Telemetry Lakehouse Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-F3722C.svg)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-OSS-00A9E0.svg)](https://delta.io/)
[![CI/CD](https://img.shields.io/badge/Deployed_via-Databricks_Asset_Bundles-00A9E0.svg)]()

A mission-critical, event-driven Lakehouse architecture designed to ingest, cleanse, and monitor high-velocity jet engine telemetry data in near real-time. 

---

## Background & Problem Statement
**The Problem:** Modern aircraft engines generate millions of telemetry pings per flight. Historically, this data was processed in massive overnight batches. If an engine's high-pressure compressor began overheating mid-flight, maintenance crews wouldn't be alerted until the next day, leading to delayed grounding decisions, potential safety risks, and catastrophic engine failure.

**The Requirement:** The business requires a fault-tolerant pipeline capable of ingesting raw JSON sensor arrays continuously, marrying that stream with static ERP dimension data (fleet safety thresholds), isolating physically impossible sensor glitches without crashing the pipeline, and alerting the control room of anomalies within a strict 5-minute SLA.

## The Solution & Value Proposition
To resolve this, we implemented a **Real-Time Medallion Lakehouse**. 
This solution perfectly fits the problem because it bridges the gap between streaming and batch. By utilizing 5-minute micro-batches, we achieve near real-time anomaly detection at a fraction of the compute cost of an always-on 24/7 continuous stream. The business gets immediate alerts, and data scientists get perfectly pristine historical data for predictive maintenance modeling.

## Architecture & Data Flow
The platform implements a strict Medallion Architecture:

<img width="840" height="433" alt="image" src="https://github.com/user-attachments/assets/246285f8-82fe-4265-b663-d5c58c7d5cad" />


1. **Bronze Layer (Raw):** * **Stream:** Continuous ingestion of raw, cryptic JSON telemetry data from cloud storage.
   * **Batch:** Daily scheduled ingestion of ERP aircraft dimension files (engine IDs, safety limits).
2. **Silver Layer (Cleansed & Quarantined):** * Transforms and casts cryptic sensor arrays into domain-specific nomenclature.
   * **Data Quality Gate:** Evaluates streaming data for physical impossibilities.
3. **Gold Layer (Aggregated & Actionable):** * **Alerts Stream:** A continuous stream-static join comparing live telemetry against ERP safety thresholds, pushing breaches to a critical alerts table.
   * **OBT Batch:** A daily One-Big-Table (OBT) aggregation for Business Intelligence reporting.

## Data Pipeline Mechanics: Ingestion & Transformation

### 1. Data Sources & Ingestion (Bronze)
* **Telemetry Stream:** Jet engine sensors publish high-velocity JSON payloads to cloud object storage (AWS S3 / Azure ADLS). We ingest this raw data using **PySpark Structured Streaming**. To handle evolving schemas and optimize file discovery, the pipeline utilizes Databricks Auto Loader (`cloudFiles`).
* **ERP Dimensions:** Static aircraft data (engine specifications, safe operating thresholds) is exported daily from enterprise systems (like SAP/Oracle) as CSV/Parquet files and ingested via standard PySpark batch reads.

### 2. Processing & Transformation (Silver)
Once the raw JSON lands in the Bronze table, the Silver streaming job performs several critical transformations:
* **Schema Enforcement & Casting:** The nested JSON arrays are flattened, and datatypes are strictly cast (e.g., strings to `TimestampType` and `FloatType`).
* **Column Renaming:** Cryptic engineering codes from the sensors (e.g., `s3`, `s4`) are renamed to domain-readable business logic (`temp_hpc_out`, `press_hpc_out`).
* **The Dead Letter Queue (DLQ):** The stream utilizes PySpark's `foreachBatch` function. Before writing the micro-batch to the Silver table, the code evaluates the dataframe against physical realities (e.g., Engine RPM cannot be negative). Valid records are appended to the main Silver Fact table, while invalid records are dynamically routed to a `slv_quarantine_telemetry` table.

### 3. Aggregation & Action (Gold)
* **Real-Time Anomaly Detection:** The Gold streaming job performs a **Stream-Static Join**. It takes the live, cleansed telemetry stream from Silver and joins it against the static ERP thresholds table. If a live temperature exceeds the `critical_limit` defined in the ERP, the record is immediately written to `gld_critical_alerts`.
* **Business Intelligence (OBT):** A separate daily batch job flattens the Silver telemetry and ERP dimension tables into a wide One-Big-Table (OBT), optimized for downstream BI tools like Tableau or PowerBI to query without requiring complex joins.

## Technology Stack & Rationale
* **Apache Spark (PySpark):** Chosen for its Structured Streaming capabilities, allowing us to process millions of records with exactly-once fault tolerance.
* **Delta Lake:** Required for its ACID transactions. It allows our Gold stream to safely read from the Silver table at the exact same time our Silver stream is writing to it.
* **Databricks Workflows & Asset Bundles (DABs):** Chosen to move the platform from "ClickOps" to "DevOps." DABs allow us to define our infrastructure as code (IaC) for safe deployments.
* **Databricks SQL Alerts:** Chosen to natively push webhook notifications to downstream communication channels (Slack/Teams).

## ⚙️ Key Engineering Implementations
* **The Dead Letter Queue (DLQ):** Rather than blindly dropping bad data, the Silver streaming pipeline utilizes PySpark's `foreachBatch` method to intercept the micro-batch, evaluate it, and route mathematically impossible readings (e.g., negative RPMs) to an isolated `slv_quarantine_telemetry` table for engineering audit.
* **Schema Enforcement & Evolution:** Strict schema enforcement on the Silver Fact table prevents upstream JSON changes from corrupting the Lakehouse, while `mergeSchema` is enabled on the Quarantine table for maximum debugging flexibility.

---

## Getting Started & Deployment

This project completely bypasses the UI by utilizing Databricks Asset Bundles (`databricks.yml`). 

### 1. Prerequisites
* A Databricks Workspace.
* The Databricks CLI installed and authenticated on your local machine.
* Python 3.10+ installed locally.

### 2. Infrastructure Deployment (CI/CD)
Clone the repository and deploy the pipeline definitions to your cloud workspace:

`git clone https://github.com/<your-username>/aerospace-telemetry-platform.git`
`cd aerospace-telemetry-platform`
`databricks bundle validate`
`databricks bundle deploy -t dev`

### 3. Running the Pipeline
To start simulating the live jet engine telemetry:

`pip install -r requirements.txt`
`python producer/engine_streaming.py`

Once the simulator is running, trigger the orchestrator via the CLI:

`databricks bundle run telemetry_streaming_engine -t dev`

## Observability & Monitoring
* **Data Audits:** If a sensor malfunctions, investigate the quarantine logs at: `aerospace.silver.slv_quarantine_telemetry`.
* **Real-Time Alerts:** Active alerts for engine overheating are published via Databricks SQL monitoring the `aerospace.gold.gld_critical_alerts` table and dispatched via webhook.
