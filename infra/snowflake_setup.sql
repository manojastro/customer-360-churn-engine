-- File: infra/snowflake_setup.sql
/*
    One-time (idempotent) provisioning for the Customer 360 & Churn Engine.

    Role model:
      CHURN_ADMIN       - owns databases/warehouses, grants everything
      CHURN_ENGINEER    - dbt + Snowpark development and deployment
      CHURN_READER      - BI / read-only consumption of analytics + scores

    Execute as SYSADMIN with SECURITYADMIN privileges available
    (snowsql -f infra/snowflake_setup.sql). Safe to re-run.
*/

-- =============================================================================
-- 1. ROLES
-- =============================================================================
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS CHURN_ADMIN;
CREATE ROLE IF NOT EXISTS CHURN_ENGINEER;
CREATE ROLE IF NOT EXISTS CHURN_READER;

GRANT ROLE CHURN_ENGINEER TO ROLE CHURN_ADMIN;
GRANT ROLE CHURN_ADMIN     TO ROLE SYSADMIN;   -- ownership hierarchy

-- =============================================================================
-- 2. WAREHOUSES
-- =============================================================================
USE ROLE SYSADMIN;
ALTER SESSION SET ABORT_DETACHED_QUERY = TRUE;

CREATE WAREHOUSE IF NOT EXISTS WH_TRANSFORM
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 3600
    COMMENT = 'dbt transformations';

CREATE WAREHOUSE IF NOT EXISTS WH_ML_SCORING
    WAREHOUSE_SIZE = MEDIUM
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MAX_CONCURRENCY_LEVEL = 4
    COMMENT = 'Snowpark stored procedure execution (churn scoring)';

-- =============================================================================
-- 3. DATABASES + SCHEMAS
-- =============================================================================
CREATE DATABASE IF NOT EXISTS SALESFORCE_RAW COMMENT = 'Replicated CRM data (Fivetran)';
CREATE DATABASE IF NOT EXISTS SUPPORT_RAW    COMMENT = 'Replicated help-desk tickets';
CREATE DATABASE IF NOT EXISTS PRODUCT_RAW    COMMENT = 'Product telemetry events';
CREATE DATABASE IF NOT EXISTS ANALYTICS      COMMENT = 'dbt-managed analytics layer';
CREATE DATABASE IF NOT EXISTS ML             COMMENT = 'ML artifacts + churn scores';

CREATE SCHEMA IF NOT EXISTS ANALYTICS.STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MARTS;
CREATE SCHEMA IF NOT EXISTS ML.PUBLIC;   -- stage ARTIFACTS + CUSTOMER_CHURN_SCORES live here

-- Raw schemas are created by the replication tooling; ensure they exist so grants below succeed.
CREATE SCHEMA IF NOT EXISTS SALESFORCE_RAW.RAW;
CREATE SCHEMA IF NOT EXISTS SUPPORT_RAW.RAW;
CREATE SCHEMA IF NOT EXISTS PRODUCT_RAW.RAW;

-- =============================================================================
-- 4. STAGE (model artifacts + sproc modules)
-- =============================================================================
CREATE STAGE IF NOT EXISTS ML.PUBLIC.ARTIFACTS
    COMMENT = 'Churn pipeline: python modules + serialized model'
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE SKIP_HEADER = 0);  -- binary/json passthrough

-- =============================================================================
-- 5. PRIVILEGES - role -> warehouse/database usage
-- =============================================================================
USE ROLE SECURITYADMIN;

GRANT USAGE ON WAREHOUSE WH_TRANSFORM  TO ROLE CHURN_ENGINEER;
GRANT USAGE ON WAREHOUSE WH_ML_SCORING TO ROLE CHURN_ENGINEER;
GRANT OPERATE ON WAREHOUSE WH_ML_SCORING TO ROLE CHURN_ENGINEER;

GRANT USAGE ON DATABASE ANALYTICS      TO ROLE CHURN_ENGINEER;
GRANT USAGE ON DATABASE SALESFORCE_RAW TO ROLE CHURN_ENGINEER;
GRANT USAGE ON DATABASE SUPPORT_RAW    TO ROLE CHURN_ENGINEER;
GRANT USAGE ON DATABASE PRODUCT_RAW    TO ROLE CHURN_ENGINEER;
GRANT USAGE ON DATABASE ML             TO ROLE CHURN_ENGINEER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE CHURN_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ML        TO ROLE CHURN_ENGINEER;
GRANT CREATE TABLE ON SCHEMA ML.PUBLIC           TO ROLE CHURN_ENGINEER;
GRANT CREATE STAGE ON SCHEMA ML.PUBLIC           TO ROLE CHURN_ENGINEER;
GRANT CREATE PROCEDURE ON SCHEMA ML.PUBLIC       TO ROLE CHURN_ENGINEER;
GRANT READ  ON STAGE ML.PUBLIC.ARTIFACTS         TO ROLE CHURN_ENGINEER;
GRANT WRITE ON STAGE ML.PUBLIC.ARTIFACTS         TO ROLE CHURN_ENGINEER;

-- Reader: consume marts + scores only.
GRANT USAGE ON DATABASE ANALYTICS TO ROLE CHURN_READER;
GRANT USAGE ON DATABASE ML        TO ROLE CHURN_READER;
GRANT USAGE ON SCHEMA ANALYTICS.MARTS TO ROLE CHURN_READER;
GRANT USAGE ON SCHEMA ML.PUBLIC       TO ROLE CHURN_READER;

-- =============================================================================
-- 6. FUTURE GRANTS (new objects automatically accessible)
-- =============================================================================
USE ROLE SYSADMIN;

GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.MARTS TO ROLE CHURN_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.MARTS TO ROLE CHURN_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.STAGING TO ROLE CHURN_ENGINEER;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA ANALYTICS.STAGING TO ROLE CHURN_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ML.PUBLIC TO ROLE CHURN_READER;

-- Replication service roles (adjust to your loader's role, e.g. FIVETRAN_ROLE):
-- GRANT USAGE ON SCHEMA SALESFORCE_RAW.RAW TO ROLE FIVETRAN_ROLE;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA SALESFORCE_RAW.RAW TO ROLE CHURN_ENGINEER;

-- =============================================================================
-- 7. SCORES TABLE (created here so readers can be granted before first run)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ML.PUBLIC.CUSTOMER_CHURN_SCORES (
    CUSTOMER_KEY              VARCHAR(64)  NOT NULL,
    CHURN_PROBABILITY_SCORE   NUMBER(9, 6) NOT NULL,
    RISK_TIER                 VARCHAR(10)  NOT NULL,
    MODEL_VERSION             VARCHAR(60),
    FEATURE_SNAPSHOT          NUMBER(4, 0),
    SCORED_AT                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CHURN_SCORES PRIMARY KEY (CUSTOMER_KEY)
)
COMMENT = 'Daily churn propensity scores written by SP_CUSTOMER_CHURN_SCORING (upsert on customer_key)'
CLUSTER BY (SCORED_AT);

GRANT SELECT ON TABLE ML.PUBLIC.CUSTOMER_CHURN_SCORES TO ROLE CHURN_READER;
