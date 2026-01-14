
CREATE SCHEMA IF NOT EXISTS THRIVE_RAW;

CREATE OR REPLACE TABLE RAW.TC_DATA (
    TRANS_ID STRING,
    TCTYPE STRING,
    CREATEDAT TIMESTAMP,
    CUSTOMERID STRING,
    AMOUNT NUMBER(10,2)
);


CREATE OR REPLACE TABLE RAW.Sales


CREATE OR REPLACE TABLE THRIVE_RAW.TC_EXPIRED LIKE THRIVE_RAW.TC_EARNED;

-- ============================================================================
-- Thrive Cash FIFO Matching Project - Database Setup
-- ============================================================================
-- This script creates the database structure for the Thrive Cash processing
-- pipeline, including raw data tables, staging tables, analytics tables,
-- and quality check/audit tables.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CREATE DATABASE AND SCHEMAS
-- ----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS THRIVE_CASH_PROJECT;
USE DATABASE THRIVE_CASH_PROJECT;

-- Raw data layer - stores unmodified source data
CREATE SCHEMA IF NOT EXISTS RAW_DATA;

-- Staging layer - cleaned and validated data
CREATE SCHEMA IF NOT EXISTS STAGING;

-- Analytics layer - final business-ready outputs
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Quality checks and monitoring
CREATE SCHEMA IF NOT EXISTS QUALITY_CHECKS;

-- ----------------------------------------------------------------------------
-- 2. RAW DATA TABLES
-- ----------------------------------------------------------------------------
    
    -- TC_Data: Thrive Cash transactions (earned, spent, expired)
    CREATE OR REPLACE TABLE RAW_DATA.TC_DATA (
        TRANS_ID NUMBER(38,0) NOT NULL,
        TCTYPE VARCHAR(50),
        CREATEDAT TIMESTAMP_NTZ,
        EXPIREDAT TIMESTAMP_NTZ,
        CUSTOMERID NUMBER(38,0) NOT NULL,
        ORDERID NUMBER(38,0),
        AMOUNT NUMBER(18,2) NOT NULL,
        REASON VARCHAR(255),
        LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (TRANS_ID)
    );
    
    -- Sales: Order and sales data
    CREATE OR REPLACE TABLE RAW_DATA.SALES (
        ORDERID NUMBER(38,0) NOT NULL,
        CUSTOMERID NUMBER(38,0) NOT NULL,
        PREDISCOUNTGROSSPRODUCTSALES NUMBER(18,2),
        ORDERWEIGHT NUMBER(18,2),
        LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ORDERID)
    );
    
    -- Customers: Customer information
    CREATE OR REPLACE TABLE RAW_DATA.CUSTOMERS (
        CUSTOMERID NUMBER(38,0) NOT NULL,
        EMAIL VARCHAR(255),
        FIRSTNAME VARCHAR(255),
        BILLINGPOSTCODE NUMBER(10,0),
        LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (CUSTOMERID)
    );


-- ----------------------------------------------------------------------------
-- 3. STAGING TABLES

tc_data_with_redemptions.csv
-- PROCESSED ---------------------------------------
-- Validated TC_Data ready for processing
    CREATE OR REPLACE TABLE PROCESSED.TC_DATA_WITH_REDEMPTIONS (
        TRANS_ID NUMBER(38,0) NOT NULL,
        TCTYPE VARCHAR(50) NOT NULL,
        CREATEDAT TIMESTAMP_NTZ NOT NULL,
        EXPIREDAT TIMESTAMP_NTZ,
        CUSTOMERID NUMBER(38,0) NOT NULL,
        ORDERID NUMBER(38,0),
        AMOUNT NUMBER(18,2) NOT NULL,
        REASON VARCHAR(255),
        VALIDATION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        REDEEMID NUMBER(38, 0),
        PRIMARY KEY (TRANS_ID)
    );


-- ----------------------------------------------------------------------------
-- 4. ANALYTICS TABLES
-- ----------------------------------------------------------------------------

-- Customer balances over time for finance reporting
    CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_BALANCES (
        CUSTOMERID NUMBER(38,0) NOT NULL,
        TRANSACTION_DATE DATE NOT NULL,
        CUMULATIVE_EARNED NUMBER(18,2) NOT NULL,
        CUMULATIVE_SPENT NUMBER(18,2) NOT NULL,
        CUMULATIVE_EXPIRED NUMBER(18,2) NOT NULL,
        CURRENT_BALANCE NUMBER(18,2) NOT NULL,
        CALCULATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (CUSTOMERID, TRANSACTION_DATE)
    );

-- Daily customer balance snapshot (current state)
    CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_BALANCE_CURRENT (
        CUSTOMERID NUMBER(38,0) NOT NULL,
        CURRENT_BALANCE NUMBER(18,2) NOT NULL,
        TOTAL_EARNED NUMBER(18,2) NOT NULL,
        TOTAL_SPENT NUMBER(18,2) NOT NULL,
        TOTAL_EXPIRED NUMBER(18,2) NOT NULL,
        LAST_TRANSACTION_DATE TIMESTAMP_NTZ,
        AS_OF_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (CUSTOMERID)
    );
    

    -- ----------------------------------------------------------------------------
-- 5. QUALITY CHECK TABLES
-- ----------------------------------------------------------------------------

    -- Data quality validation results
    CREATE OR REPLACE TABLE QUALITY_CHECKS.VALIDATION_RESULTS (
        VALIDATION_ID NUMBER IDENTITY(1,1),
        VALIDATION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        VALIDATION_TYPE VARCHAR(100) NOT NULL,  -- 'SOURCE', 'RESULTS', 'RECONCILIATION'
        TABLE_NAME VARCHAR(255) NOT NULL,
        CHECK_NAME VARCHAR(255) NOT NULL,
        CHECK_STATUS VARCHAR(50) NOT NULL,      -- 'PASS', 'FAIL', 'WARNING'
        RECORDS_CHECKED NUMBER(38,0),
        RECORDS_FAILED NUMBER(38,0),
        ERROR_MESSAGE VARCHAR(5000),
        PRIMARY KEY (VALIDATION_ID)
    );
    
-- Error log for stored procedures and tasks
    CREATE OR REPLACE TABLE QUALITY_CHECKS.ERROR_LOG (
        ERROR_ID NUMBER IDENTITY(1,1),
        ERROR_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PROCEDURE_NAME VARCHAR(255) NOT NULL,
        ERROR_CODE VARCHAR(50),
        ERROR_MESSAGE VARCHAR(5000),
        ERROR_STACKTRACE VARCHAR(10000),
        PRIMARY KEY (ERROR_ID)
    );

-- Pipeline execution audit log
    CREATE OR REPLACE TABLE QUALITY_CHECKS.PIPELINE_AUDIT (
        AUDIT_ID NUMBER IDENTITY(1,1),
        RUN_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        TASK_NAME VARCHAR(255) NOT NULL,
        STATUS VARCHAR(50) NOT NULL,            -- 'STARTED', 'SUCCESS', 'FAILED'
        DURATION_SECONDS NUMBER(18,2),
        RECORDS_PROCESSED NUMBER(38,0),
        NOTES VARCHAR(5000),
        PRIMARY KEY (AUDIT_ID)
    );

-- Reconciliation checks - ensures FIFO matching is correct
    CREATE OR REPLACE TABLE QUALITY_CHECKS.RECONCILIATION_RESULTS (
        RECON_ID NUMBER IDENTITY(1,1),
        RECON_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        CUSTOMERID NUMBER(38,0) NOT NULL,
        TOTAL_EARNED NUMBER(18,2),
        TOTAL_REDEEMED NUMBER(18,2),           -- spent + expired
        EXPECTED_BALANCE NUMBER(18,2),
        ACTUAL_BALANCE NUMBER(18,2),
        VARIANCE NUMBER(18,2),
        STATUS VARCHAR(50),                     -- 'BALANCED', 'VARIANCE'
        PRIMARY KEY (RECON_ID)
    );
    
