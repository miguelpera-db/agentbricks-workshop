# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab - Build a Knowledge Assistant with Agent Bricks
# MAGIC In this lab, you will learn how to build, configure, and interact with a Knowledge Assistant using Agent Bricks on the Databricks platform. A Knowledge Assistant is a specialized AI agent that can answer questions using data connected through Unity Catalog and powered by Vector Search.
# MAGIC
# MAGIC ## Learning objectives
# MAGIC _By the end of this lab, you will be able to:_
# MAGIC - Configure and deploy a Knowledge Assistant entirely through the Databricks UI
# MAGIC - Connect a Knowledge Assistant to governed data sources using Vector Search and Unity Catalog
# MAGIC - Interact with the deployed agent in the Playground to retrieve product-specific information with citations
# MAGIC - Explore datasets and validate that the assistant provides accurate, relevant responses
# MAGIC - Understand how to evaluate and improve assistant quality through SME feedback and built-in review tools
# MAGIC - Understand how to query the assistant endpoint programmatically (SQL, curl, Python) for integration into downstream workflows

# COMMAND ----------

# MAGIC %run "../../Includes/Classroom-Setup-4.1L"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Create the Knowledge Assistant
# MAGIC This part of the lab will be configured by your instructor. Please follow along, but this part of the lab will be performed for you so you can query the Agent Brick deployed within this Workspace.
# MAGIC
# MAGIC 1. In the left nav, go to **AI / ML → Agents → Agent Bricks → Knowledge Assistant**.
# MAGIC    (Or search “Knowledge Assistant” in the workspace search.)
# MAGIC 2. On **Configure**, fill in:
# MAGIC
# MAGIC    - **Name**: my-product-agent
# MAGIC    - **Description**: An agent used to answer questions regarding customer product information. 
# MAGIC 3. **Add knowledge source** (the core of your RAG):
# MAGIC      - **Type**: *Vector Search Index*
# MAGIC      - **Source**: product_docs_index 
# MAGIC      - **Doc URI Column**: product_id
# MAGIC      - **Text Column**: indexed_doc
# MAGIC      - **Describe the content**: Product documentation
# MAGIC 4. (Optional) **Instructions**: Style/guardrails for answers (tone, citation behavior, etc.). 
# MAGIC 5. Legacy (skipped)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. 🚨FOR LAB INSTRUCTORS ONLY🚨
# MAGIC
# MAGIC 6. 🚨**FOR LAB INSTRUCTORS ONLY**🚨 Click **Create agent**. The right-side panel will show build/sync progress and, once ready, links to the **deployed endpoint**, **experiment**, and **synced sources**. (Initial build/sync can take a while.)
# MAGIC
# MAGIC > Please allow for 5-10 minutes for the agent to be deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Using the Brick
# MAGIC 7.  Navigate to the Playground using the left menu. 
# MAGIC 8. Using the model name dropdown menu at the top, select your model. You can search for `my-product-agent`. 
# MAGIC 9. Start querying! Here are some sample questions you can ask
# MAGIC       - "Can you tell me about the BlendMaster Elite 4000?"

# COMMAND ----------

# MAGIC %md
# MAGIC Note the citations presented as a part of the output. This brings in the power of Agent Bricks; your agent is connected to your data and completely governed by Unity Catalog. It's in this way that your agent lives alongside your data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Explore the Dataset and Test your Brick
# MAGIC You can reference different product names by running the cell below.

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_name from product_docs where product_name is not null

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Evaluation and Improvement
# MAGIC 1. Navigate back to Agents and click on the brick `my-product-agent`.
# MAGIC 1. At the top of the screen, click on **Improve Quality**. Here users can add questions for labeling sessions, allowing subject matter experts (SMEs) to review and provide feedback. The insights gained from these sessions are crucial for improving the agent's responses.
# MAGIC > This falls outside the scope of this Get Started course, but you can read more about the SME review process and how SME feedback is easily incorporated into Agent Bricks capabilities [here](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/knowledge-assistant#step-3-improve-quality).

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Querying The Endpoint
# MAGIC There are multiple ways to query the created knowledge assistant endpoint. Use the code examples provided in AI Playground as a starting point.
# MAGIC
# MAGIC ### Instructions
# MAGIC - On the Configure tab, click Open in playground.
# MAGIC - From Playground, click Get code.
# MAGIC - Select Python API for a code example to interact with the endpoint using Python.
# MAGIC - Copy the `model` value. For example, it will look like  `model="ka-XXXXXXXX-endpoint"`. This can also be found, for example, by navigating to the Agents menu as well and taking note of the value `ka-XXXXXXXX-endpoint`.
# MAGIC - Paste the model endpoint value into the cell below and run the query _Can you tell me about the BlendMaster Elite 4000?_ in the following cell. 
# MAGIC
# MAGIC > You need to fill in `model="<FILL_IN>"` down below.

# COMMAND ----------

model="<FILL_IN>"

# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
base_url=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
base_url = f"https://{base_url}/serving-endpoints"

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=base_url
)

response = client.responses.create(
    model=model,
    input=[
        {
            "role": "user",
            "content": "Can you tell me about the BlendMaster Elite 4000?"
        }
    ]
)

print(response.output[0].content[0].text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Next Steps
# MAGIC This lab only covered one type of agents. There are still [more agents](https://docs.databricks.com/aws/en/generative-ai/agent-bricks#supported-use-cases) to explore for various use cases like information extraction or multi-agent supervisor. Another example of a supported agent type is [Information Extraction](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/key-info-extraction#what-is-agent-bricks-information-extraction). With this agent, you can optionally choose to have [Databricks compare multiple different optimization strategies](https://docs.databricks.com/aws/en/generative-ai/agent-bricks#supported-use-cases) to build and recommend and optimized agent via foundation model fine-tuning. You can use this lab as a launching point for starting your own POC!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up
# MAGIC If you _did_ create an agent using the instructions above, please navigate to your agent using the **Agents** menu on the left side of the workspace, select the three vertical dots, and select **delete**. This helps preserve resources for other users in this lab environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨FOR LAB INSTRUCTORS ONLY🚨
# MAGIC #### Instructions
# MAGIC 1. Please change clean_up to True before logging out the workspace if you are an instructor.
# MAGIC 1. Please run the next cell to clean up any Bricks deployed during this lab.

# COMMAND ----------

clean_up = False # Set to false by default in case a user selects Run all by accident.
if clean_up:
  run_agent_bricks_cleanup()
else:
  print("No Bricks have been cleaned.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC By completing this lab, you have gained hands-on experience with the full lifecycle of a Knowledge Assistant on Databricks. You saw how Agent Bricks enable you to rapidly deploy agents that stay aligned with enterprise governance and data security through Unity Catalog. You also learned how to interact with your assistant in the Playground, validate its responses with citations, and explore mechanisms for continuous improvement using SME feedback. With these skills, you are now ready to extend Knowledge Assistants to your own datasets and apply them to real-world scenarios, accelerating how your organization retrieves and uses knowledge with production-ready agents.
# MAGIC ### Where can I learn more about Agent Bricks? 
# MAGIC Check out our official documentation and resources:
# MAGIC 1. [Agent Bricks by Databricks: Your No-Code Path to Smarter Automation](https://www.databricks.com/resources/demos/tours/data-science-and-ai/agent-bricks?itm_data=demo_center)
# MAGIC 1. [Production AI agents optimized on your data](https://www.databricks.com/product/artificial-intelligence/agent-bricks)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
