# Accounting_Data_Processing
Example of Accounting Processing using Snowflake 

# Thrive Cash FIFO Pipeline
<img width="1761" height="851" alt="image" src="https://github.com/user-attachments/assets/18970496-057c-4315-86b9-69d477311df6" />

# Accounting_Data_Processing  
**Example of Accounting Processing using Snowflake**
---

## Overview

This project demonstrates a **production-grade accounting data pipeline** for the Thrive Cash rewards program. The goal is to automate the reconciliation of *earned*, *spent*, and *expired* Thrive Cash transactions while enforcing **FIFO (First-In, First-Out)** accounting rules.

The pipeline replaces a manual, error-prone month-end close process with a **fully automated, scalable Snowflake solution** that:

- Ingests raw transaction data from CSV files
- Validates and standardizes records
- Applies FIFO matching logic at scale
- Produces analytics-ready datasets for reporting and reconciliation

The design emphasizes **accuracy, auditability, and scalability** for millions of transactions.

---

## Architecture & Orchestration

The pipeline is orchestrated using **Snowflake Tasks** as a lightweight alternative to Airflow.

### Key Components

- **Raw Layer**
  - CSV files loaded from Snowflake stages using `COPY INTO`
- **Staging Layer**
  - Validated and standardized transaction data
- **Processed Layer**
  - FIFO-matched, analytics-ready tables
- **Orchestration**
  - Snowflake Tasks with `AFTER` dependencies forming a DAG

### Task Flow (DAG)

1. `run_tc_pipeline` – Root orchestration task  
2. `load_tc_data_csv` – Load raw CSV files into staging tables  
3. `populate_tc_data_validated` – Insert new validated records  
4. `fifo_match_task` – Apply FIFO matching logic via stored procedure  
5. `populate_tc_data_processed` – Final processed output  

This approach provides:

- Deterministic execution order
- On-demand or scheduled execution
- Clear data lineage without external orchestration tools

---

## FIFO Matching Logic

The FIFO logic assigns each **spent or expired transaction** to the **oldest unmatched earned transaction** for the same customer.

### Matching Rules

- Matching is performed **per customer**
- Earned transactions are ordered by `CREATEDAT` (FIFO)
- Each earned `TRANS_ID` can only be redeemed **once**
- If no earned transaction exists, `REDEEMID` remains `NULL`

### Implementation Approach

- Window functions (`ROW_NUMBER`) assign sequence numbers to earned and spent transactions per customer
- Matching occurs where `spent_seq = earn_seq`
- A single set-based `UPDATE` applies the results

This avoids row-by-row processing and scales efficiently to **millions of records**.

---

## Data Quality & Validation

Data quality checks are applied before FIFO matching to ensure accounting accuracy:

- Required fields (`TRANS_ID`, `TCTYPE`, `CUSTOMERID`, `AMOUNT`) are enforced
- Duplicate `TRANS_ID`s are prevented using primary keys
- Only new transactions are inserted into validated tables
- Validation timestamps provide auditability

These controls reduce downstream errors and support month-end close requirements.

---

## Data Modeling & Transformations

The data model follows a **layered architecture**:

- **Raw tables** preserve source data
- **Validated tables** enforce schema, constraints, and deduplication
- **Processed tables** contain business logic outputs (FIFO matching)

Transformations are written in **SQL** and structured to be easily migrated to **dbt models**, including:

- Incremental logic
- Clear separation of staging vs. business logic
- Reusable and testable transformations

---

## Analytics & Reporting

The final processed table enables:

- Customer-level Thrive Cash balances
- Earned vs. redeemed reconciliation
- Identification of unmatched or expired balances
- Audit-friendly reporting for accounting and finance teams

The output is analytics-ready and can be consumed directly by:

- BI tools (Tableau, Looker, Power BI)
- Financial close workflows
- Downstream analytics pipelines

---

## How to Run This Project

### Prerequisites

- Snowflake account
- Warehouse with task execution permissions
- Database and schemas created

### Execution Steps

1. **Create stages and file formats**
2. **Load raw data**
   ```sql
   EXECUTE TASK load_tc_data_csv;

