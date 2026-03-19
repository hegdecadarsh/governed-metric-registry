-- ============================================================================
-- Git Integration Setup for Metric Registry App
-- Creates API integration, Git repository, and management procedures
-- ============================================================================

USE ROLE ACCOUNTADMIN;


-- =====================================================
-- METRIC REGISTRY WITH AI GOVERNANCE - DEPLOYMENT SCRIPT
-- =====================================================

-- Step 1: Create Database and Schema
CREATE DATABASE IF NOT EXISTS METRIC_REGISTRY_DB;
CREATE SCHEMA IF NOT EXISTS METRIC_REGISTRY_DB.REGISTRY;

-- Step 2: Create the Metric Registry Table
CREATE OR REPLACE TABLE METRIC_REGISTRY (
    METRIC_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    METRIC_NAME VARCHAR(255) NOT NULL UNIQUE,
    DOMAIN VARCHAR(100) NOT NULL,
    METRIC_SQL VARCHAR(50000) NOT NULL,
    DESCRIPTION VARCHAR(5000),
    AI_DESCRIPTION VARCHAR(5000),
    SUGGESTED_DIMENSIONS VARCHAR(5000),
    SUGGESTED_MEASURES VARCHAR(5000),
    POTENTIAL_RISKS VARCHAR(5000),
    RISK_LEVEL VARCHAR(20),
    AI_RECOMMENDATIONS VARCHAR(5000),
    STATUS VARCHAR(50) DEFAULT 'DRAFT',
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    OWNER_EMAIL VARCHAR(255),
    APPROVED_BY VARCHAR(255),
    APPROVED_AT TIMESTAMP_NTZ,
    APPROVAL_COMMENT VARCHAR(2000),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    VISIBLE_TO_ROLES VARCHAR(255),
    HIDDEN_FROM_ROLES VARCHAR(255),
    ACCESS_SCOPE VARCHAR(255),
    ACCESS_MESSAGE VARCHAR(255)

);

-- ============================================================================
-- 1. API Integration for Git (Public Repository - No Auth)
-- ============================================================================
CREATE OR REPLACE API INTEGRATION metric_registry_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;

-- ============================================================================
-- 2. Git Repository Clone
--    Replace the ORIGIN URL with your actual repository URL
-- ============================================================================
CREATE OR REPLACE GIT REPOSITORY METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO
  API_INTEGRATION = metric_registry_git_api_integration
  ORIGIN = 'https://github.com/hegdecadarsh/governed-metric-registry.git';

-- ============================================================================
-- 3. Stored Procedures
-- ============================================================================

-- ---------------------------------------------------------------------------
-- FETCH_GIT_UPDATES: Pull latest changes from the remote Git repository
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.FETCH_GIT_UPDATES()
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN
  ALTER GIT REPOSITORY METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO FETCH;
  RETURN 'Git repository fetched successfully at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- ---------------------------------------------------------------------------
-- LIST_GIT_BRANCHES: Show all available branches in the repository
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.LIST_GIT_BRANCHES()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  res RESULTSET DEFAULT (SHOW GIT BRANCHES IN METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO);
BEGIN
  RETURN TABLE(res);
END;
$$;

-- ---------------------------------------------------------------------------
-- LIST_GIT_TAGS: Show all tags in the repository
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.LIST_GIT_TAGS()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  res RESULTSET DEFAULT (SHOW GIT TAGS IN METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO);
BEGIN
  RETURN TABLE(res);
END;
$$;

-- ---------------------------------------------------------------------------
-- LIST_GIT_FILES: List files at a given path/branch in the repository
--   branch_or_tag: e.g. 'main', 'v1.0'
--   subpath:       e.g. '/' or '/src'
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.LIST_GIT_FILES(
  branch_or_tag VARCHAR,
  subpath VARCHAR DEFAULT '/'
)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  full_path VARCHAR;
  res RESULTSET;
BEGIN
  full_path := '@METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO/branches/' || branch_or_tag || subpath;
  res := (EXECUTE IMMEDIATE 'LS ''' || full_path || '''');
  RETURN TABLE(res);
END;
$$;

-- ---------------------------------------------------------------------------
-- DEPLOY_STREAMLIT_FROM_GIT: Deploy the Metric Registry Streamlit app
--   from a specific branch in the Git repository
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.DEPLOY_STREAMLIT_FROM_GIT(
  branch_name VARCHAR DEFAULT 'main',
  app_name VARCHAR DEFAULT 'METRIC_REGISTRY_APP',
  warehouse_name VARCHAR DEFAULT 'COMPUTE_WH',
  main_file VARCHAR DEFAULT 'metric_registry_app.py'
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  git_path VARCHAR;
  stage_path VARCHAR;
  create_stage_sql VARCHAR;
  copy_sql VARCHAR;
  create_app_sql VARCHAR;
  set_wh_sql VARCHAR;
BEGIN
  ALTER GIT REPOSITORY METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO FETCH;

  git_path := '@METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO/branches/' || branch_name || '/';
  stage_path := '@METRIC_REGISTRY_DB.REGISTRY.STREAMLIT_DEPLOY_STAGE';

  CREATE OR REPLACE STAGE METRIC_REGISTRY_DB.REGISTRY.STREAMLIT_DEPLOY_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

  COPY FILES
    INTO stage_path
    FROM git_path
    PATTERN = '.*\\.py';

  COPY FILES
    INTO stage_path
    FROM git_path
    PATTERN = '.*environment\\.yml';

  create_app_sql := 'CREATE OR REPLACE STREAMLIT METRIC_REGISTRY_DB.REGISTRY.' || app_name ||
    ' ROOT_LOCATION = ''' || stage_path || '''' ||
    ' MAIN_FILE = ''' || main_file || '''' ||
    ' QUERY_WAREHOUSE = ''' || warehouse_name || '''';
  EXECUTE IMMEDIATE create_app_sql;

  RETURN 'Streamlit app ' || app_name || ' deployed from branch ' || branch_name || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- ---------------------------------------------------------------------------
-- GET_GIT_FILE_CONTENT: Read content of a specific file from the Git repo
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.GET_GIT_FILE_CONTENT(
  branch_name VARCHAR DEFAULT 'main',
  file_path VARCHAR DEFAULT 'metric_registry_app.py'
)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  full_path VARCHAR;
  query_sql VARCHAR;
  res RESULTSET;
BEGIN
  full_path := '@METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO/branches/' || branch_name || '/' || file_path;
  query_sql := 'SELECT $1 AS FILE_CONTENT FROM ''' || full_path || ''' (FILE_FORMAT => ''METRIC_REGISTRY_DB.REGISTRY.GIT_TEXT_FORMAT'')';

  CREATE OR REPLACE FILE FORMAT METRIC_REGISTRY_DB.REGISTRY.GIT_TEXT_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = NONE
    ESCAPE_UNENCLOSED_FIELD = NONE;

  res := (EXECUTE IMMEDIATE query_sql);
  RETURN TABLE(res);
END;
$$;

-- ---------------------------------------------------------------------------
-- COMPARE_GIT_VERSIONS: Compare deployed version vs latest in Git
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.COMPARE_GIT_VERSIONS(
  branch_name VARCHAR DEFAULT 'main'
)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  ALTER GIT REPOSITORY METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO FETCH;

  res := (
    SELECT
      git.name AS GIT_FILE,
      git.size AS GIT_SIZE,
      git.last_modified AS GIT_MODIFIED,
      deployed.name AS DEPLOYED_FILE,
      deployed.size AS DEPLOYED_SIZE,
      deployed.last_modified AS DEPLOYED_MODIFIED,
      CASE
        WHEN deployed.name IS NULL THEN 'NEW_IN_GIT'
        WHEN git.size != deployed.size THEN 'MODIFIED'
        ELSE 'UNCHANGED'
      END AS STATUS
    FROM (
      SELECT * FROM DIRECTORY(@METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO)
      WHERE RELATIVE_PATH LIKE 'branches/' || :branch_name || '/%.py'
    ) git(name, size, md5, last_modified, etag)
    LEFT JOIN (
      SELECT * FROM DIRECTORY(@METRIC_REGISTRY_DB.REGISTRY.STREAMLIT_DEPLOY_STAGE)
    ) deployed(name, size, md5, last_modified, etag)
      ON SPLIT_PART(git.name, '/', -1) = SPLIT_PART(deployed.name, '/', -1)
  );
  RETURN TABLE(res);
END;
$$;

-- ---------------------------------------------------------------------------
-- ROLLBACK_STREAMLIT_DEPLOY: Redeploy app from a specific Git tag or branch
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.ROLLBACK_STREAMLIT_DEPLOY(
  tag_or_branch VARCHAR,
  is_tag BOOLEAN DEFAULT FALSE,
  app_name VARCHAR DEFAULT 'METRIC_REGISTRY_APP',
  warehouse_name VARCHAR DEFAULT 'COMPUTE_WH',
  main_file VARCHAR DEFAULT 'metric_registry_app.py'
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  git_path VARCHAR;
  stage_path VARCHAR;
  create_app_sql VARCHAR;
BEGIN
  ALTER GIT REPOSITORY METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO FETCH;

  IF (is_tag) THEN
    git_path := '@METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO/tags/' || tag_or_branch || '/';
  ELSE
    git_path := '@METRIC_REGISTRY_DB.REGISTRY.METRIC_REGISTRY_REPO/branches/' || tag_or_branch || '/';
  END IF;

  stage_path := '@METRIC_REGISTRY_DB.REGISTRY.STREAMLIT_DEPLOY_STAGE';

  CREATE OR REPLACE STAGE METRIC_REGISTRY_DB.REGISTRY.STREAMLIT_DEPLOY_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

  COPY FILES
    INTO stage_path
    FROM git_path
    PATTERN = '.*\\.py';

  COPY FILES
    INTO stage_path
    FROM git_path
    PATTERN = '.*environment\\.yml';

  create_app_sql := 'CREATE OR REPLACE STREAMLIT METRIC_REGISTRY_DB.REGISTRY.' || app_name ||
    ' ROOT_LOCATION = ''' || stage_path || '''' ||
    ' MAIN_FILE = ''' || main_file || '''' ||
    ' QUERY_WAREHOUSE = ''' || warehouse_name || '''';
  EXECUTE IMMEDIATE create_app_sql;

  RETURN 'Streamlit app ' || app_name || ' rolled back to ' ||
    IFF(is_tag, 'tag', 'branch') || ' ' || tag_or_branch || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- ---------------------------------------------------------------------------
-- LOG_GIT_DEPLOYMENT: Record deployment history to an audit table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS METRIC_REGISTRY_DB.REGISTRY.GIT_DEPLOYMENT_LOG (
  DEPLOYMENT_ID NUMBER AUTOINCREMENT,
  BRANCH_OR_TAG VARCHAR,
  APP_NAME VARCHAR,
  DEPLOYED_BY VARCHAR DEFAULT CURRENT_USER(),
  DEPLOYED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  STATUS VARCHAR,
  DETAILS VARCHAR
);

CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.DEPLOY_STREAMLIT_WITH_LOGGING(
  branch_name VARCHAR DEFAULT 'main',
  app_name VARCHAR DEFAULT 'METRIC_REGISTRY_APP',
  warehouse_name VARCHAR DEFAULT 'COMPUTE_WH',
  main_file VARCHAR DEFAULT 'metric_registry_app.py'
)
  RETURNS VARCHAR
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  result_msg VARCHAR;
BEGIN
  BEGIN
    CALL METRIC_REGISTRY_DB.REGISTRY.DEPLOY_STREAMLIT_FROM_GIT(:branch_name, :app_name, :warehouse_name, :main_file) INTO result_msg;

    INSERT INTO METRIC_REGISTRY_DB.REGISTRY.GIT_DEPLOYMENT_LOG (BRANCH_OR_TAG, APP_NAME, STATUS, DETAILS)
    VALUES (:branch_name, :app_name, 'SUCCESS', :result_msg);

    RETURN result_msg;
  EXCEPTION
    WHEN OTHER THEN
      INSERT INTO METRIC_REGISTRY_DB.REGISTRY.GIT_DEPLOYMENT_LOG (BRANCH_OR_TAG, APP_NAME, STATUS, DETAILS)
      VALUES (:branch_name, :app_name, 'FAILED', SQLERRM);
      RETURN 'Deployment failed: ' || SQLERRM;
  END;
END;
$$;

-- ---------------------------------------------------------------------------
-- GET_DEPLOYMENT_HISTORY: View past deployments
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE METRIC_REGISTRY_DB.REGISTRY.GET_DEPLOYMENT_HISTORY(
  limit_rows NUMBER DEFAULT 20
)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  res RESULTSET DEFAULT (
    SELECT * FROM METRIC_REGISTRY_DB.REGISTRY.GIT_DEPLOYMENT_LOG
    ORDER BY DEPLOYED_AT DESC
    LIMIT :limit_rows
  );
BEGIN
  RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- Usage Examples
-- ============================================================================
-- CALL METRIC_REGISTRY_DB.REGISTRY.FETCH_GIT_UPDATES();
-- CALL METRIC_REGISTRY_DB.REGISTRY.LIST_GIT_BRANCHES();
-- CALL METRIC_REGISTRY_DB.REGISTRY.LIST_GIT_FILES('main', '/');
-- CALL METRIC_REGISTRY_DB.REGISTRY.DEPLOY_STREAMLIT_WITH_LOGGING('main');
-- CALL METRIC_REGISTRY_DB.REGISTRY.GET_DEPLOYMENT_HISTORY(10);
-- CALL METRIC_REGISTRY_DB.REGISTRY.ROLLBACK_STREAMLIT_DEPLOY('v1.0', TRUE);
-- CALL METRIC_REGISTRY_DB.REGISTRY.GET_GIT_FILE_CONTENT('main', 'metric_registry_app.py');
-- CALL METRIC_REGISTRY_DB.REGISTRY.COMPARE_GIT_VERSIONS('main');

