# Databricks notebook source
# MAGIC %md
# MAGIC # NERC Setup
# MAGIC
# MAGIC This notebook sets up the workspace for AI Agents on Databricks including:
# MAGIC - Validating catalog and schema structure
# MAGIC - Loading PDF files into Volume

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize Environment and Imports

# COMMAND ----------

# MAGIC %pip install -qqq databricks-vectorsearch backoff databricks-openai uv databricks-agents mlflow-skinny[databricks]
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from databricks.vector_search.client import VectorSearchClient

print("Starting workspace setup...")

# COMMAND ----------

user_schema=spark.sql("SELECT regexp_replace(session_user(), '@.*$', '');").collect()[0][0]
print(f"User schema: {user_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Catalog, Schema and Volumes

# COMMAND ----------

def validate_catalog_and_schema_and_volumes():
    """Validate that required catalog, schema, and volume exist"""
    try:
        # Check if catalog exists
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        if "dbacademy" not in catalogs:
            raise Exception("Catalog 'dbacademy' does not exist. Please create it before running this setup.")
        
        print("Catalog 'dbacademy' found")
        
        # Check if schema exists
        schemas = [row.databaseName for row in spark.sql("SHOW SCHEMAS IN dbacademy").collect()]
        if user_schema not in schemas:
            raise Exception(f"Schema 'dbacademy.{user_schema}' does not exist. Please create it before running this setup.")
        
        print(f"Schema 'dbacademy.{user_schema}' found")
        
        # Check if volume exists
        volume_name = f"nerc"
        volumes = [row.volume_name for row in spark.sql(f"SHOW VOLUMES IN dbacademy.{user_schema}").collect()]
        if volume_name not in volumes:
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' does not exist. Creating volume...")
            spark.sql(f"CREATE VOLUME  dbacademy.{user_schema}.`{volume_name}`")
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' created")
        else:
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' found")
        
        return True
    except Exception as e:
        print(f"Error validating catalog/schema/volume: {str(e)}")
        raise e

setup_success = validate_catalog_and_schema_and_volumes()
spark.sql("USE CATALOG dbacademy;")
spark.sql(f"USE SCHEMA {user_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load PDF Files into Volumes

# COMMAND ----------

import os
import urllib.request

volume_path = f"/Volumes/dbacademy/{user_schema}/nerc"

pdf_files = [
    "https://www.nerc.com/globalassets/standards/projects/2023-06/2023-06_cip-014_risk_assessment_refinement_sar_redline_01312024.pdf",
    "https://www.nerc.com/globalassets/standards/projects/2023-06/2023-06_cip-014_risk_assessment_refinement_sar_clean_01312024.pdf"
]

for download_url in pdf_files:
    file_name = os.path.basename(download_url)
    target_path = f"{volume_path}/{file_name}"
    print(f"Downloading {download_url}...")

    # Add a User-Agent header to avoid 403 error
    req = urllib.request.Request(
        download_url,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req) as response:
        file_content = response.read()

    # Write binary content as base64 to dbutils.fs
    import base64
    encoded_content = base64.b64encode(file_content).decode("utf-8")
    dbutils.fs.put(target_path, encoded_content, overwrite=True)
    print(f"Downloaded {file_name} to {target_path}")
