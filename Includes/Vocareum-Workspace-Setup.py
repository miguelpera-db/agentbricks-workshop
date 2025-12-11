# Databricks notebook source
!pip install -U -qqq unitycatalog-ai[databricks]
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# Already done in workspace setup
# function_dict = {
#     "get_order_history": f"""
#     -- Create a function to get order history by user
#     CREATE OR REPLACE FUNCTION get_order_history(
#     email STRING COMMENT 'Email of the user to retrieve order history'
#     )
#     RETURNS TABLE (
#     returns_last_12_months INT,
#     issue_category STRING, 
#     todays_date DATE
#     )
#     COMMENT 'This takes the user_name of a customer as an input and returns the number of returns and the issue category'
#     LANGUAGE SQL
#     RETURN(
#     SELECT count(*) as returns_last_12_months, issue_category, now() as todays_date
#     FROM cust_service_data 
#     WHERE email = email
#     GROUP BY issue_category
#     );
#     """,
#     "get_return_policy": f"""
#     -- Create a function to retrieve company policy details
#     CREATE OR REPLACE FUNCTION get_return_policy(
#     policy_name STRING COMMENT 'Policy name to return. Example policies: Account Cancellation Policy, Exchange Policy, Refund Policy, Warranty Policy, Privacy Policy, Return Policy'
#     )
#     RETURNS TABLE (
#     policy           STRING,
#     policy_details   STRING,
#     last_updated     DATE
#     )
#     COMMENT 'Returns the details of the Return Policy'
#     LANGUAGE SQL
#     RETURN (
#     SELECT
#     policy,
#     policy_details,
#     last_updated
#     FROM policies
#     WHERE policy = policy_name
#     LIMIT 1
#     );
#     """,
#     "get_latest_return": f"""
#     -- Create a function to get the latest return request
#     CREATE OR REPLACE FUNCTION get_latest_return()
#     RETURNS TABLE(
#     purchase_date DATE, issue_category STRING, issue_description STRING, name STRING
#     )
#     COMMENT 'Returns the most recent customer service interaction, such as returns.'
#     RETURN (
#     SELECT 
#         CAST(date_time AS DATE) AS purchase_date,
#         issue_category,
#         issue_description,
#         name
#     FROM cust_service_data
#     ORDER BY date_time DESC
#     LIMIT 1
#     );
#     """,
#     "search_product_docs": f"""
#     -- Create a function to search product documentation using vector search
#     CREATE OR REPLACE FUNCTION search_product_docs(
#     search_term STRING COMMENT 'Search term for finding relevant product documentation'
#     )
#     RETURNS TABLE
#     COMMENT 'Searches product documentation using vector search to retrieve relevant documentation excerpts for troubleshooting and support. This should be used to search by product as each product has its own documentation.'
#     RETURN(
#     SELECT
#         product_name,
#         indexed_doc as doc
#     FROM
#         vector_search(
#         index => 'dbacademy.{user_schema}.product_docs_index',
#         query => search_term,
#         num_results => 1
#     )
#     );
#     """
# }

# COMMAND ----------

# def check_if_lab_user():
#     username = spark.sql("SELECT current_user()").collect()[0][0]
#     # Find if user is labuser
#     return username, username.startswith('labuser')

# def clean_uc_functions(function_dict: dict):
#     function_list = list(function_dict.keys())

#     username, check_if_lab_user_bool = check_if_lab_user()
#     for function in function_list: 
#         print(function)
#         if check_if_lab_user_bool:
#             pass 
#         else:
#             print("You are in a Dev environment")
#             print("Cleaning UC assets for testing.")
#             spark.sql(f'DROP FUNCTION IF EXISTS `{DA.catalog_name}.{DA.schema_name}.{function}`')
#             print("✅ Successfully cleaned out UC functions.")

# COMMAND ----------

def function_creation_check(function_name:str, sql_query: str):
    try:
        spark.sql(sql_query)
        print(f"✅ Function {function_name} created successfully")
    except Exception as e:
        print(f"❌ Failed to create function {function_name}. Exception: {e}")

# COMMAND ----------

# clean_uc_functions(function_dict)

# COMMAND ----------

# Already done in workspace setup
# function_list = list(function_dict.keys())
# for function in function_list:
#     function_creation_check(function, function_dict[function])

# COMMAND ----------

print(user_schema)

# COMMAND ----------


