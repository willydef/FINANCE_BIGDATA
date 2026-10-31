# FINANCE_BIGDATA
### Banking Transactions Data Engineering & Analytics Pipeline

A hands-on Big Data engineering project designed to process, validate, transform, and analyze banking transaction data using **Python, Apache Spark (PySpark), Apache Kafka, Docker, Apache Parquet, and Microsoft Power BI**.

The project demonstrates batch ETL processing, transaction quality controls, a separate Kafka ingestion proof of concept, analytical reporting, and interactive business intelligence dashboards.

## 1. Project Objectives

- Build a banking data processing pipeline using PySpark.
- Extract and transform customer, account, and transaction datasets.
- Validate transaction records and identify invalid data.
- Store processed datasets in Apache Parquet format.
- Experiment with Kafka-based transaction ingestion.
- Generate analytical datasets for transaction and risk reporting.
- Visualize banking activity through Power BI.
- Automate batch ETL execution and analytical data exports.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.9 | Data processing and pipeline orchestration |
| Apache Spark / PySpark 3.5.7 | Distributed data processing framework |
| Apache Kafka 3.9.1 | Transaction event ingestion proof of concept |
| Docker | Kafka container deployment |
| Ubuntu Server | Kafka hosting environment |
| Apache Parquet | Columnar data storage |
| Pandas | CSV export for Power BI |
| Microsoft Power BI | Interactive analytics dashboard |
| Windows PowerShell | Local execution and administration |

## 3. Project Architecture

The project currently contains two processing workflows.

### Batch Analytics Pipeline

```text
Banking CSV Files
        |
        v
PySpark ETL
        |
        v
Data Cleaning & Transformation
        |
        v
Parquet Storage
        |
        v
Banking Analytics & Risk Reporting
        |
        v
Power BI CSV Exports
        |
        v
Power BI Dashboard
```

### Kafka Transaction Processing — Separate Proof of Concept

```text
Transaction JSON Events
        |
        v
Apache Kafka (Docker / Ubuntu)
        |
        v
PySpark Kafka Reader
        |
        v
Schema Parsing & Validation
        |
        +---------------------+
        |                     |
        v                     v
Valid Transactions     Rejected Transactions
        |                     |
        v                     v
Parquet Output          Parquet Output
```

The Kafka workflow is implemented separately and is not yet connected to the automated batch analytics pipeline.

## 4. Project Structure

```text
FINANCE_BIGDATA/
|
|-- data/
|   |-- clients.csv
|   |-- comptes.csv
|   |-- transactions.csv
|   |-- etl_transactions.parquet/
|   |-- transactions_enrichies.parquet/
|   |-- rapport_risques_clients.parquet/
|
|-- src/
|   |-- 01_test_spark.py
|   |-- 02_lire_transactions.py
|   |-- 03_controle_qualite.py
|   |
|   |-- etl/
|       |-- extract.py
|       |-- transform.py
|       |-- load.py
|       |-- pipeline.py
|
|-- output/
|   |-- transactions_valides/
|   |-- transactions_rejetees/
|
|-- powerbi/
|   |-- transactions_enrichies.csv
|   |-- rapport_risques_clients.csv
|   |-- FINANCE_BIGDATA_Dashboard.pbix
|
|-- docs/
|-- sql/
|-- tests/
|
|-- export_powerbi.py
|-- run_finance_bigdata.py
|-- README.md
```

Additional Kafka processing scripts are maintained as part of the project environment.

## 5. Batch ETL Pipeline

The ETL workflow is implemented in Python using PySpark.

**Extract**

Read customer, account, and transaction data from CSV files.

**Transform**

Apply data transformations, enrich transactions with banking reference data, and perform quality checks.

**Load**

Write the processed transaction dataset to Apache Parquet.

The completed ETL test processed:

- 10 customers
- 10 bank accounts
- 150 transactions
- 150 enriched transaction records

The validation checks reported no duplicate transactions, missing mandatory fields, or invalid transaction amounts in the tested batch.

## 6. Banking Analytics

The analytical processing script generates two Parquet datasets:

**transactions_enrichies.parquet**

Enriched transaction information, including customer, account, transaction amount, transaction type, and associated attributes.

**rapport_risques_clients.parquet**

Aggregated reporting information organized by risk level, including transaction counts, customer counts, and total transaction amounts.

The resulting datasets are exported to CSV for Power BI consumption.

## 7. Kafka Transaction Validation

Apache Kafka is deployed in a Docker container on an Ubuntu Server virtual machine.

A PySpark Kafka reader retrieves JSON transaction events and applies a defined schema.

The validation workflow checks transaction identifiers, customer identifiers, and positive transaction amounts.

In the proof-of-concept test, one valid transaction and one invalid transaction were processed and separated into different Parquet outputs.

This demonstrates Kafka ingestion and batch validation of Kafka records, not a fully integrated real-time streaming analytics pipeline.

## 8. Power BI Dashboard

The Power BI dashboard includes:

- Total transaction count
- Total transaction amount
- Transaction amounts by transaction type
- Transaction amounts by risk level
- Interactive city filtering

The tested dashboard displayed **150 transactions** and approximately **163.62K TND** in transaction amounts.

The dashboard reads CSV files exported from the analytical Parquet datasets. Refreshing the Power BI Desktop report is currently a manual step.

## 9. Running the Pipeline

### Prerequisites

- Python 3.9
- PySpark 3.5.7
- Pandas 2.0.3
- Java 17
- Compatible Hadoop