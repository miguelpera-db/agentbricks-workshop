# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup
# MAGIC
# MAGIC This notebook sets up the workspace for AI Agents on Databricks including:
# MAGIC - Validating catalog and schema structure
# MAGIC - Loading CSV data into Delta tables
# MAGIC - Setting up permissions
# MAGIC - Configuring vector search endpoint
# MAGIC - Create vector search endpoint

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
        volume_name = f"customer-service"
        volumes = [row.volume_name for row in spark.sql(f"SHOW VOLUMES IN dbacademy.{user_schema}").collect()]
        if volume_name not in volumes:
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' does not exist. Creating volume...")
            spark.sql(f"CREATE VOLUME dbacademy.{user_schema}.`{volume_name}`")
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' created")
        else:
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' found")
        

        # Check if volume exists
        volume_name = f"product-docs"
        volumes = [row.volume_name for row in spark.sql(f"SHOW VOLUMES IN dbacademy.{user_schema}").collect()]
        if volume_name not in volumes:
            print(f"Volume 'dbacademy.{user_schema}.{volume_name}' does not exist. Creating volume...")
            spark.sql(f"CREATE VOLUME dbacademy.{user_schema}.`{volume_name}`")
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
# MAGIC ## 3. Load CSV Files to Volumes and Delta Tables and PDFs/MDs to Volume

# COMMAND ----------

import urllib.request

base_url = "https://raw.githubusercontent.com/databricks/tmm/main/agents-workshop/data"
volume_path = f"/Volumes/dbacademy/{user_schema}/customer-service"
csv_files = ["cust_service_data.csv", "policies.csv", "product_docs.csv"]

for file_name in csv_files:
    download_url = f"{base_url}/{file_name}"
    target_path = f"{volume_path}/{file_name}"
    
    # Download file content from URL
    with urllib.request.urlopen(download_url) as response:
        file_content = response.read()
    
    # Write to volume using dbutils
    dbutils.fs.put(target_path, file_content.decode('utf-8'), overwrite=True)
    print(f"Downloaded {file_name} to {target_path}")

# COMMAND ----------

def load_csv_to_delta():
    """Load CSV files from volume to Delta tables"""
    
    if not setup_success:
        print("Skipping CSV loading due to catalog/schema validation failure")
        return False
    
    # Check existing tables
    try:
        existing_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN dbacademy.{user_schema}").collect()]
    except:
        existing_tables = []
    
    # Define source files and target tables
    csv_files = [
        ("cust_service_data.csv", "cust_service_data"),
        ("policies.csv", "policies"),
        ("product_docs.csv", "product_docs")
    ]
    
    volume_path = f"/Volumes/dbacademy/{user_schema}/customer-service"
    
    loaded_tables = []
    skipped_tables = []
    
    for csv_file, table_name in csv_files:
        try:
            # Check if table already exists
            if table_name in existing_tables:
                print(f"Table {table_name} already exists, skipping...")
                skipped_tables.append(table_name)
                continue
            
            file_path = f"{volume_path}/{csv_file}"
            
            # Check if file exists
            try:
                dbutils.fs.ls(file_path)
            except:
                print(f"File not found: {file_path}")
                continue
            
            print(f"Reading {csv_file}...")
            
            # Read CSV file
            df = spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True,
                quote='"',
                escape='"',
                multiLine=True
            )
            
            # Show basic info about the data
            row_count = df.count()
            print(f"   Rows: {row_count}")
            print(f"   Columns: {len(df.columns)}")
            
            # Write as Delta table
            table_path = f"dbacademy.{user_schema}.{table_name}"
            
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .option("mergeSchema", "true") \
              .saveAsTable(table_path)
            
            print(f"Created Delta table: {table_path}")
            loaded_tables.append(table_name)
            
        except Exception as e:
            print(f"Error loading {csv_file}: {str(e)}")
    
    if skipped_tables:
        print(f"\nSkipped existing tables: {', '.join(skipped_tables)}")
    if loaded_tables:
        print(f"Successfully loaded {len(loaded_tables)} new tables: {', '.join(loaded_tables)}")
    
    # Return True if we have any tables (existing or newly loaded)
    return len(loaded_tables) > 0 or len(skipped_tables) > 0

tables_loaded = load_csv_to_delta()

# COMMAND ----------

import os
import urllib.request

volume_path = f"/Volumes/dbacademy/{user_schema}/product-docs"

pdf_files = [
    "https://raw.githubusercontent.com/miguelpera-db/agentbricks-workshop/main/resources/product_doc_blendmaster_elite_4000.pdf",
    "https://raw.githubusercontent.com/miguelpera-db/agentbricks-workshop/main/resources/product_doc_brownbox_X500.md"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Set Permissions

# COMMAND ----------

def setup_permissions(tables: list):
    """Grant schema and table permissions to all users"""
    
    if not tables_loaded:
        print("Skipping permissions setup - no tables were loaded")
        return False
    
    try:
        # Grant USE SCHEMA permission
        # spark.sql(f"GRANT USE SCHEMA ON SCHEMA dbacademy.{user_schema} TO `account users`")
        # print("Granted USE SCHEMA permission to all users")
        
        # # Grant SELECT permission on each table
        # tables = ["cust_service_data", "policies", "product_docs"]
        
        for table in tables:
            try:
                spark.sql(f"GRANT SELECT ON TABLE dbacademy.{user_schema}.{table} TO `account users`")
                print(f"Granted SELECT permission on {table}")
            except Exception as e:
                print(f"Could not grant permission on {table}: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"Error setting up permissions: {str(e)}")
        return False

def enable_cdf():
    """
    Enable Change Data Feed (CDF) for dbacademy.{user_schema}.product_docs table
    and create a vector search index.
    """
    print(f"Enabling CDF on dbacademy.{user_schema}.product_docs table...")
    try:
        # Try to enable CDC via ALTER TABLE
        spark.sql(f"""
            ALTER TABLE dbacademy.{user_schema}.product_docs
            SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        print(f"CDF enabled on dbacademy.{user_schema}.product_docs table")
        return True
    except Exception as e:
        print(f"Error enabling CDC: {e}")
        return False


# Grant SELECT permission on each table
tables = ["cust_service_data", "policies", "product_docs"]
permissions_set = setup_permissions(tables)
cdf_enabled = enable_cdf()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vector Search Endpoint Setup

# COMMAND ----------



# COMMAND ----------

def setup_vector_search_endpoint():
    """Check and create vector search endpoint if needed"""
    
    try:
        vs_client = VectorSearchClient()
        endpoint_name = f"vs_endpoint_{user_schema}"
        
        # List existing endpoints
        existing_endpoints = vs_client.list_endpoints()
        endpoint_names = [ep['name'] for ep in existing_endpoints.get('endpoints', [])]
        
        # Check if endpoint exists
        if endpoint_name in endpoint_names:
            print(f"Vector search endpoint '{endpoint_name}' already exists")
            return True
        else:
            print(f"Creating vector search endpoint '{endpoint_name}'...")
            
            # Create the endpoint
            vs_client.create_endpoint(
                name=endpoint_name,
                endpoint_type="STANDARD"
            )
            
            print(f"Created vector search endpoint '{endpoint_name}'")
            return True
            
    except Exception as e:
        print(f"Error with vector search endpoint: {str(e)}")
        return False

vs_endpoint_ready = setup_vector_search_endpoint()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Vector Search Index
# MAGIC
# MAGIC

# COMMAND ----------

import time
import sys
# import logging

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s %(levelname)s %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)]
# )
# logger = logging.getLogger(__name__)

def ensure_endpoint(client: VectorSearchClient, endpoint_name: str,
                    timeout: int = 1200, poll_interval: int = 10):
    try:
        eps = client.list_endpoints().get("endpoints", [])
        names = [ep["name"] for ep in eps]
        print(f"Existing endpoints: {names}")
        if endpoint_name not in names:
            print(f"Creating vector search endpoint '{endpoint_name}'")
            client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
        else:
            print(f"Endpoint '{endpoint_name}' already exists")

        start = time.time()
        while True:
            ep_info = client.get_endpoint(endpoint_name)
            state = ep_info.get("endpoint_status", {}).get("state", "UNKNOWN")
            print(f"Endpoint state: {state}")
            if state in ("ONLINE", "PROVISIONED", "READY"):
                print(f"Endpoint '{endpoint_name}' ready")
                break
            if state in ("FAILED", "OFFLINE"):
                raise RuntimeError(f"Endpoint '{endpoint_name}' failed (state={state})")
            if time.time() - start > timeout:
                raise TimeoutError(f"Timed out waiting for endpoint '{endpoint_name}' to be ready")
            time.sleep(poll_interval)
    except Exception as e:
        print(f"Error ensuring endpoint: {e}")
        raise

# Check if the index exists
def index_exists(vsc, vs_endpoint_name, index_name):
  try:
      dict_vsindex = vsc.get_index(vs_endpoint_name, index_name).describe()
      return dict_vsindex.get('status').get('ready', False)
  except Exception as e:
      if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
          print(f'Unexpected error describing the index. This could be a permission issue.')
          raise e
  return False

# Inform the user that the index is ready
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(360):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# Enable CDS for the source table and create a vector search index
def create_vs_index():
    # create or sync the index
    vsc = VectorSearchClient()
    vs_endpoint_name = f"vs_endpoint_{user_schema}"
    vs_index_name = f"dbacademy.{user_schema}.product_docs_index"
    source_table = f"dbacademy.{user_schema}.product_docs"

    ensure_endpoint(vsc, vs_endpoint_name)

    if not index_exists(vsc, vs_endpoint_name, vs_index_name):
        print(f"Creating index {vs_index_name} on endpoint {vs_endpoint_name}...")
        
        try:
            vsc.create_delta_sync_index(
                endpoint_name=vs_endpoint_name,
                source_table_name=source_table,
                index_name=vs_index_name,
                pipeline_type="TRIGGERED",
                primary_key="product_id",
                embedding_source_column="indexed_doc",
                embedding_model_endpoint_name="databricks-gte-large-en"
            )
            print(f"Vector search index '{vs_index_name}' created")
            return True
        except Exception as e:
            print(f"Error creating vector search index: {e}")
            print("Note: Index may already exist")
            return False
    else:
        print("Index already exists")
        return True
        #Trigger a sync to update our vs content with the new data saved in the table
        #vsc.get_index(vs_endpoint_name, vs_index_fullname).sync()

    #Let's wait for the index to be ready and all our embeddings to be created and indexed
    wait_for_index_to_be_ready(vsc, vs_endpoint_name, vs_index_name)


vs_index_ready = create_vs_index()

# COMMAND ----------

tables = ["product_docs_index"]
permissions_set = setup_permissions(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Functions
# MAGIC

# COMMAND ----------

display(spark.sql(f'USE dbacademy.{user_schema}'))

# COMMAND ----------

# DBTITLE 1,Create dictionary of functions
# The tables referenced below are created as a part of the setup above. 

function_dict = {
    "get_latest_return": f"""
    -- Create a function to get the latest return request
    CREATE OR REPLACE FUNCTION dbacademy.{user_schema}.get_latest_return()
    RETURNS TABLE(
    purchase_date DATE, issue_category STRING, issue_description STRING, name STRING
    )
    COMMENT 'Returns the most recent customer service interaction, such as returns.'
    RETURN (
    SELECT 
        CAST(date_time AS DATE) AS purchase_date,
        issue_category,
        issue_description,
        name
    FROM dbacademy.{user_schema}.cust_service_data
    ORDER BY date_time DESC
    LIMIT 1
    );
    """,
    "search_product_docs": f"""
    -- Create a function to search product documentation using vector search
    CREATE OR REPLACE FUNCTION dbacademy.{user_schema}.search_product_docs(
    search_term STRING COMMENT 'Search term for finding relevant product documentation'
    )
    RETURNS TABLE
    COMMENT 'Searches product documentation using vector search to retrieve relevant documentation excerpts for troubleshooting and support. This should be used to search by product as each product has its own documentation.'
    RETURN(
    SELECT
        product_name,
        indexed_doc as doc
    FROM
        vector_search(
        index => 'dbacademy.{user_schema}.product_docs_index',
        query => search_term,
        num_results => 1
    )
    );
    """,
    "get_order_history": f"""
    -- Create a function to get order history by user
    CREATE OR REPLACE FUNCTION dbacademy.{user_schema}.get_order_history(
    email STRING COMMENT 'Email of the user to retrieve order history'
    )
    RETURNS TABLE (
    returns_last_12_months INT,
    issue_category STRING, 
    todays_date DATE
    )
    COMMENT 'This takes the user_name of a customer as an input and returns the number of returns and the issue category'
    LANGUAGE SQL
    RETURN(
    SELECT count(*) as returns_last_12_months, issue_category, now() as todays_date
    FROM dbacademy.{user_schema}.cust_service_data 
    WHERE email = email
    GROUP BY issue_category
    );
            """,
    "get_return_policy": f"""
    -- Create a function to retrieve company policy details
    CREATE OR REPLACE FUNCTION get_return_policy(
    policy_name STRING COMMENT 'Policy name to return. Example policies: Account Cancellation Policy, Exchange Policy, Refund Policy, Warranty Policy, Privacy Policy, Return Policy'
    )
    RETURNS TABLE (
    policy           STRING,
    policy_details   STRING,
    last_updated     DATE
    )
    COMMENT 'Returns the details of the Return Policy'
    LANGUAGE SQL
    RETURN (
    SELECT
    policy,
    policy_details,
    last_updated
    FROM dbacademy.{user_schema}.policies
    WHERE policy = policy_name
    LIMIT 1
    );
    """
}

# COMMAND ----------

# DBTITLE 1,creates the function
def function_creation_check(function_name:str, sql_query: str):
    try:
        spark.sql(sql_query)
        print(f"✅ Function {function_name} created successfully")
    except:
        print(f"❌ Failed to create function {function_name}")

def grant_permissions_to_func(function_name:str):
    try:
        spark.sql(f"""
                GRANT EXECUTE ON FUNCTION dbacademy.{user_schema}.{function_name} TO `account users`
                """
                )
        print(f"✅ Function {function_name} granted to all users")
    except Exception as e:
        print(f"❌ Failed to grant permission to function {function_name}")

function_list = list(function_dict.keys())
for function in function_list:
    function_creation_check(function, function_dict[function]) # Check function was created
    grant_permissions_to_func(function) # Check that function is accessible

# COMMAND ----------

# DBTITLE 1,Deploy the agent
# %run "./admin_driver"

# COMMAND ----------

# DBTITLE 1,Update Query-Only Permissions
# import json, requests

# endpoint_name = "agents_dbacademy-datasets-demo-agent-model"

# # Get endpoint ID
# from mlflow.deployments import get_deploy_client
# model_serving_client = get_deploy_client("databricks")
# endpoint_id = model_serving_client.get_endpoint(endpoint_name).id

# # Auth + URL
# DATABRICKS_INSTANCE = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
# TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# url = f"{DATABRICKS_INSTANCE}/api/2.0/permissions/serving-endpoints/{endpoint_id}"
# headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# # Grant CAN_QUERY (adjust principals as needed)
# payload = {
#     "access_control_list": [
#         {"group_name": "users", "permission_level": "CAN_QUERY"}
#     ]
# }

# # Apply update
# resp = requests.patch(url, headers=headers, data=json.dumps(payload))
# resp.raise_for_status()
# print("Updated permissions to CAN_QUERY.")

# COMMAND ----------

# DBTITLE 1,Enable Scale to Zero

def enable_scale_to_zero(endpoint_name: str) -> None:
    """
    Enables scale_to_zero for all served items on a serving endpoint and preserves current traffic config.
    Handles both 'served_models' and 'served_entities' shapes.
    """


    # Auth + base URL from the current notebook context
    DATABRICKS_INSTANCE = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Fetch current endpoint config
    get_url = f"{DATABRICKS_INSTANCE}/api/2.0/serving-endpoints/{endpoint_name}"
    resp = requests.get(get_url, headers=headers)
    resp.raise_for_status()
    endpoint_info = resp.json()

    cfg = endpoint_info.get("config", {})
    # Determine whether this endpoint uses 'served_models' (model endpoints)
    # or 'served_entities' (responses/agents or other types)
    served_key = "served_models" if "served_models" in cfg else ("served_entities" if "served_entities" in cfg else None)
    if not served_key:
        raise RuntimeError(f"Could not find 'served_models' or 'served_entities' in endpoint config for '{endpoint_name}'.")

    served_items = cfg.get(served_key, [])
    if not served_items:
        print(f"⚠️ No items found under '{served_key}' for '{endpoint_name}'. Nothing to update.")
        return

    updated_items = []
    for item in served_items:
        item_copy = copy.deepcopy(item)
        item_copy["scale_to_zero_enabled"] = True
        updated_items.append(item_copy)

    traffic_config = cfg.get("traffic_config", {})

    put_url = f"{DATABRICKS_INSTANCE}/api/2.0/serving-endpoints/{endpoint_name}/config"
    update_payload = {
        served_key: updated_items,
        "traffic_config": traffic_config
    }
    resp = requests.put(put_url, headers=headers, json=update_payload)
    resp.raise_for_status()
    print(f"✅ Enabled scale_to_zero for all items in '{served_key}' on '{endpoint_name}'.")

# COMMAND ----------

# import time

# import json
# import copy
# import requests

# # wait 20 minutes
# number_of_minutes = 20
# print(f"⏳ Waiting {number_of_minutes} minutes before enabling scale_to_zero...")
# time.sleep(20 * 60)

# try:
#     enable_scale_to_zero(endpoint_name)
# except Exception as e:
#     print(f"Error enabling scale_to_zero: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Setup Summary

# COMMAND ----------

print("="*60)
print("WORKSPACE SETUP COMPLETE")
print("="*60)

print(f"Catalog & Schema: {'Ready' if setup_success else 'Failed'}")
print(f"Delta Tables: {'Loaded' if tables_loaded else 'Failed'}")  
print(f"Permissions: {'Set' if permissions_set else 'Failed'}")
print(f"CDF: {'Enabled' if permissions_set else 'Failed'}")
print(f"Vector Search: {'Ready' if vs_endpoint_ready else 'Failed'}")
print(f"Vector Search Index: {'Ready' if vs_index_ready else 'Failed'}")

if all([setup_success, tables_loaded, permissions_set, cdf_enabled, vs_endpoint_ready, vs_index_ready]):
    print("\nAll components successfully configured!")
    print("\nAvailable tables:")
    print(f"- dbacademy.{user_schema}.cust_service_data")
    print(f"- dbacademy.{user_schema}.policies") 
    print(f"- dbacademy.{user_schema}.product_docs")
    print(f"\nVector search endpoint: vs_endpoint_{user_schema}")
    print(f"\nVector search index is created and ready for queries")
else:
    print("\nSome components failed to configure. Check logs above.")

print("="*60)
