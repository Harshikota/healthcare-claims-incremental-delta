🏥 Healthcare Claims Incremental ELT Pipeline  
 Azure Data Factory | ADLS Gen2 | Databricks | PySpark | Delta Lake

---
 📌 Project Overview

This project demonstrates an end-to-end incremental ELT pipeline for processing healthcare claims using modern Azure data engineering architecture.

The solution processes claims data incrementally using a watermark pattern and maintains the latest state of claims using Delta Lake MERGE operations.

The architecture follows the Medallion pattern:

- Bronze → Raw Ingested Data  
- Silver → Cleaned & Deduplicated Data  
- Gold → Business Ready Tables & Aggregations  

---

## 🏗️ Architecture

Source System (Azure SQL / Files)  
→ Azure Data Factory (Incremental Ingestion using Watermark)  
→ ADLS Gen2 (Raw/Bronze Layer)  
→ Azure Databricks (PySpark Transformations)  
→ Delta Lake (Silver & Gold Tables)  
→ Reporting / Analytics  

---

## 🎯 Business Problem

Healthcare claims can be updated multiple times (Pending → Approved → Rejected).

Processing full data every day is expensive and inefficient.

This solution:
- Processes only new or updated records
- Maintains the latest version of each claim
- Generates provider-level KPIs
- Ensures data quality

---

## 🧱 Medallion Data Layers

### 🥉 Bronze Layer
- Stores raw ingested data
- Adds ingestion metadata:
  - `_ingested_at`
  - `_source_file`
- Stored in Delta format

### 🥈 Silver Layer
- Standardizes schema & data types
- Cleans and normalizes status values
- Removes duplicates using Window functions
- Keeps latest record per claim_id

### 🥇 Gold Layer

#### 1️⃣ gold_claims_latest
- One latest row per claim_id
- Uses Delta MERGE for incremental upserts
- Partitioned by service_date

#### 2️⃣ gold_provider_kpis_daily
Aggregated metrics:
- total_claims
- total_billed_amt
- total_approved_amt
- avg_approved_amt
- approval_rate

---

## 🔄 Incremental Load Strategy

This project uses a Watermark pattern:

1. ADF reads last watermark value (updated_at)
2. Copies only records where updated_at > watermark
3. Data lands in ADLS raw layer
4. Databricks processes changes
5. Delta MERGE upserts into Gold table
6. Watermark is updated

This approach:
- Reduces processing cost
- Ensures idempotency
- Handles late arriving updates

---

## ⚙️ Technologies Used

- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks
- PySpark
- Delta Lake
- SQL

---

## 📊 Key PySpark Concepts Demonstrated

- Window Functions (ROW_NUMBER)
- Deduplication using partition & order
- Delta Lake MERGE (Upsert)
- Aggregations using groupBy
- Partitioning
- Data Quality Checks

---

## 🧪 Data Quality Checks

The project validates:

- Null claim_id values
- Duplicate claim_id records
- Negative billed or approved amounts
- Invalid status values

---

## 📂 Project Structure
healthcare-claims-incremental-delta/
│
├── notebooks/
│ ├── 00_setup_paths.py
│ ├── 01_bronze_ingest.py
│ ├── 02_silver_clean_dedup.py
│ ├── 03_gold_claims_latest_merge.py
│ ├── 04_gold_provider_kpis.py
│ └── 05_data_quality_checks.py
│
├── data_sample/
│ ├── claims.csv
│ ├── providers.csv
│ ├── members.csv
│ └── incremental_claims.csv
│
├── docs/
│ ├── data_model.md
│ └── architecture.md
│
└── adf/
├── pipeline_design.md
└── parameters.md

---

## 🚀 How to Run (Databricks)

1. Mount ADLS container (if required)
2. Execute notebooks in order:
   - 00_setup_paths.py
   - 01_bronze_ingest.py
   - 02_silver_clean_dedup.py
   - 03_gold_claims_latest_merge.py
   - 04_gold_provider_kpis.py
   - 05_data_quality_checks.py

---

## 🔥 Optimization & Best Practices

- Gold tables partitioned by service_date
- Delta Lake used for ACID transactions
- MERGE ensures safe incremental updates
- Small files can be compacted using OPTIMIZE
- Z-Ordering can be applied for faster filtering

---

## 💡 Interview Discussion Points

This project demonstrates:

- Incremental data loading using watermark pattern
- Deduplication using window functions
- Delta Lake MERGE operations
- Medallion architecture design
- Data quality validation
- Partition strategy and performance optimization

---

## 📌 Future Enhancements

- Implement SCD Type 2 for provider dimension
- Add CI/CD deployment pipeline
- Integrate monitoring & alerting
- Add Power BI dashboard

---

## 👩‍💻 Author

Harshitha  
Azure Data Engineer  
