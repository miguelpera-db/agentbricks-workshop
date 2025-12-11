# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

def check_serverless_version():
    import os
    assert os.environ.get("IS_SERVERLESS") == "TRUE", "You must select serverless compute for this notebook"
    assert os.environ.get("DATABRICKS_RUNTIME_VERSION").startswith("client.4"), "You must select serverless v4 to run this notebook"

#check_serverless_version()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view storing information from the obs table.
# MAGIC CREATE OR REPLACE TEMP VIEW user_info AS
# MAGIC SELECT map_from_arrays(
# MAGIC     collect_list(replace(key, '.', '_')),
# MAGIC     collect_list(value)
# MAGIC ) AS user_map
# MAGIC FROM dbacademy.ops.meta;
# MAGIC
# MAGIC -- Create SQL dictionary var (map) with explicit types
# MAGIC DECLARE OR REPLACE DA MAP<STRING, STRING>;
# MAGIC
# MAGIC -- Set the temp view in the DA variable
# MAGIC SET VAR DA = (SELECT user_map FROM user_info);
# MAGIC
# MAGIC DROP VIEW IF EXISTS user_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change the default catalog/schema
# MAGIC -- USE CATALOG dbacademy;
# MAGIC USE CATALOG IDENTIFIER(DA.catalog_name);
# MAGIC
# MAGIC -- CREATE SCHEMA IF NOT EXISTS datasets;
# MAGIC -- USE SCHEMA datasets;

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE OR REPLACE SCHM STRING;
# MAGIC SET VAR SCHM = (SELECT regexp_replace(session_user(), '@.*$', ''));
# MAGIC USE SCHEMA IDENTIFIER(SCHM);

# COMMAND ----------

user_schema=spark.sql("SELECT regexp_replace(session_user(), '@.*$', '');").collect()[0][0]
print(f"User schema: {user_schema}")

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()
