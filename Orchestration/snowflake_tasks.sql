-- This sets the order of tasks in the processing database 
USE SCHEMA ORCHESTRATION

EXECUTE TASK run_tc_pipeline

ALTER TASK run_tc_pipeline SUSPEND



ALTER TASK  load_tc_data_csv RESUME;
ALTER TASK  load_sales_csv RESUME;
ALTER TASK  load_customers_csv RESUME;
ALTER TASK  populate_tc_data_validated RESUME;
ALTER TASK  MOVE_TO_REDEMPTION_TASK RESUME;
ALTER TASK  fifo_match_task RESUME;
-- Run last 
ALTER TASK  run_tc_pipeline RESUME;


execute task run_tc_pipeline




SELECT *
FROM INFORMATION_SCHEMA.TASKS
WHERE TABLE_SCHEMA = 'ORCHESTRATION';

-- overall pipeline run 
    CREATE OR REPLACE TASK run_tc_pipeline
      WAREHOUSE = compute_wh
      SCHEDULE = 'USING CRON 0 * * * * UTC'  -- every hour
      AS
    SELECT 1;

-- LOAD CSV FILES ------------------------------------------------
CREATE OR REPLACE TASK load_tc_data_csv
  WAREHOUSE = compute_wh
  AFTER run_tc_pipeline
  AS
COPY INTO raw_data.tc_data (TRANS_ID, TCTYPE, CREATEDAT, EXPIREDAT, CUSTOMERID, ORDERID, AMOUNT, REASON)
FROM '@"THRIVE_DB"."STAGING"."RAW_STAGE"/tc_data_01142025.csv'
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)



CREATE OR REPLACE TASK load_sales_csv
  WAREHOUSE = compute_wh
  AFTER run_tc_pipeline
AS
COPY INTO raw_data.sales (ORDERID, CUSTOMERID, PREDISCOUNTGROSSPRODUCTSALES, ORDERWEIGHT)
FROM '@"THRIVE_DB"."STAGING"."RAW_STAGE"/sales.csv'
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)



CREATE OR REPLACE TASK load_customers_csv
  WAREHOUSE = compute_wh
  AFTER run_tc_pipeline
AS
COPY INTO raw_data.customers (CUSTOMERID, EMAIL, FIRSTNAME, BILLINGPOSTCODE)
FROM '@"THRIVE_DB"."STAGING"."RAW_STAGE"/customers.csv'
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)


-- VALIDATION TASKS ----------------------------------
    CREATE OR REPLACE TASK populate_tc_data_validated
      WAREHOUSE = compute_wh
      AFTER load_tc_data_csv
      AS
    INSERT INTO THRIVE_DB.STAGING.TC_DATA_VALIDATED (
        TRANS_ID,
        TCTYPE,
        CREATEDAT,
        EXPIREDAT,
        CUSTOMERID,
        ORDERID,
        AMOUNT,
        REASON
    )
    SELECT 
        t.TRANS_ID,
        t.TCTYPE,
        t.CREATEDAT,
        t.EXPIREDAT,
        t.CUSTOMERID,
        t.ORDERID,
        t.AMOUNT,
        t.REASON
    FROM THRIVE_DB.RAW_DATA.TC_DATA t
    WHERE NOT EXISTS (
        SELECT 1
        FROM THRIVE_DB.STAGING.TC_DATA_VALIDATED v
        WHERE v.TRANS_ID = t.TRANS_ID
    );

create or replace task MOVE_TO_REDEMPTION_TASK
	warehouse=COMPUTE_WH
    AFTER populate_tc_data_validated
	as CALL PROCESSED.MOVE_TC_DATA_TO_REDEMPTION_TABLE();

    
-- CALL FIFO MATCHING ---------------------------
CREATE OR REPLACE TASK fifo_match_task
  WAREHOUSE = compute_wh
  AFTER MOVE_TO_REDEMPTION_TASK
  AS
CALL PROCESSED.SP_FIFO_MATCH();




    





