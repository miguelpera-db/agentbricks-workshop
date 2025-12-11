# Databricks notebook source
# MAGIC %run "./Vocareum-Workspace-Setup"

# COMMAND ----------

function_dict = {
    "get_latest_return": f"""
    -- Create a function to get the latest return request
    CREATE OR REPLACE FUNCTION get_latest_return()
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
    FROM dbacademy.datasets.cust_service_data
    ORDER BY date_time DESC
    LIMIT 1
    );
    """,
    "search_product_docs": """
    -- Create a function to search product documentation using vector search
    CREATE OR REPLACE FUNCTION search_product_docs(
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
        index => 'dbacademy.datasets.product_docs_index',
        query => search_term,
        num_results => 1
    )
    );
    """

}

# COMMAND ----------

# clean_uc_functions(function_dict)

# COMMAND ----------

function_list = list(function_dict.keys())
for function in function_list:
    function_creation_check(function, function_dict[function])

# COMMAND ----------

# This will wipe out the file completely for lab use
open("env.json", "w").close()
