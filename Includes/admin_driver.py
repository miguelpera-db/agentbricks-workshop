# Databricks notebook source
# MAGIC %md
# MAGIC #Tool-calling Agent
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI playground export. In this notebook, you will:
# MAGIC - Author a tool-calling [MLflow's `ResponsesAgent`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent) that uses the OpenAI client
# MAGIC - Manually test the agent's output
# MAGIC - Evaluate the agent with Mosaic AI Agent Evaluation
# MAGIC - Log and deploy the agent
# MAGIC
# MAGIC This notebook should be run on serverless or a cluster with DBR<17.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses the OpenAI SDK, but AI Agent Framework is compatible with any agent authoring framework, including LlamaIndex or LangGraph. To learn more, see the [Authoring Agents](https://docs.databricks.com/generative-ai/agent-framework/author-agent) Databricks documentation.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqqq backoff databricks-openai uv databricks-agents mlflow-skinny[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

user_schema=spark.sql("SELECT regexp_replace(session_user(), '@.*$', '');").collect()[0][0]
print(f"User schema: {user_schema}")

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC import json
# MAGIC from typing import Any, Callable, Generator, Optional
# MAGIC from uuid import uuid4
# MAGIC
# MAGIC import backoff
# MAGIC import mlflow
# MAGIC import openai
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks_openai import UCFunctionToolkit, VectorSearchRetrieverTool
# MAGIC from mlflow.entities import SpanType
# MAGIC from mlflow.pyfunc import ResponsesAgent
# MAGIC from mlflow.types.responses import (
# MAGIC     ResponsesAgentRequest,
# MAGIC     ResponsesAgentResponse,
# MAGIC     ResponsesAgentStreamEvent,
# MAGIC )
# MAGIC from openai import OpenAI
# MAGIC from pydantic import BaseModel
# MAGIC from unitycatalog.ai.core.base import get_uc_function_client
# MAGIC
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
# MAGIC
# MAGIC SYSTEM_PROMPT = """"""
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define tools for your agent, enabling it to retrieve data or take actions
# MAGIC ## beyond text generation
# MAGIC ## To create and see usage examples of more tools, see
# MAGIC ## https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html
# MAGIC ###############################################################################
# MAGIC class ToolInfo(BaseModel):
# MAGIC     """
# MAGIC     Class representing a tool for the agent.
# MAGIC     - "name" (str): The name of the tool.
# MAGIC     - "spec" (dict): JSON description of the tool (matches OpenAI Responses format)
# MAGIC     - "exec_fn" (Callable): Function that implements the tool logic
# MAGIC     """
# MAGIC
# MAGIC     name: str
# MAGIC     spec: dict
# MAGIC     exec_fn: Callable
# MAGIC
# MAGIC
# MAGIC def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None):
# MAGIC     tool_spec["function"].pop("strict", None)
# MAGIC     tool_name = tool_spec["function"]["name"]
# MAGIC     udf_name = tool_name.replace("__", ".")
# MAGIC
# MAGIC     # Define a wrapper that accepts kwargs for the UC tool call,
# MAGIC     # then passes them to the UC tool execution client
# MAGIC     def exec_fn(**kwargs):
# MAGIC         function_result = uc_function_client.execute_function(udf_name, kwargs)
# MAGIC         if function_result.error is not None:
# MAGIC             return function_result.error
# MAGIC         else:
# MAGIC             return function_result.value
# MAGIC     return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)
# MAGIC
# MAGIC
# MAGIC TOOL_INFOS = []
# MAGIC
# MAGIC UC_TOOL_NAMES = [
# MAGIC     f"dbacademy.{user_schema}.get_order_history",
# MAGIC     f"dbacademy.{user_schema}.get_return_policy",
# MAGIC     ]
# MAGIC
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
# MAGIC uc_function_client = get_uc_function_client()
# MAGIC for tool_spec in uc_toolkit.tools:
# MAGIC     TOOL_INFOS.append(create_tool_info(tool_spec))
# MAGIC
# MAGIC
# MAGIC # Use Databricks vector search indexes as tools
# MAGIC # See [docs](https://docs.databricks.com/generative-ai/agent-framework/unstructured-retrieval-tools.html) for details
# MAGIC
# MAGIC # # (Optional) Use Databricks vector search indexes as tools
# MAGIC # # See https://docs.databricks.com/generative-ai/agent-framework/unstructured-retrieval-tools.html
# MAGIC # # for details
# MAGIC VECTOR_SEARCH_TOOLS = []
# MAGIC # # TODO: Add vector search indexes as tools or delete this block
# MAGIC # VECTOR_SEARCH_TOOLS.append(
# MAGIC #         VectorSearchRetrieverTool(
# MAGIC #         index_name="",
# MAGIC #         # filters="..."
# MAGIC #     )
# MAGIC # )
# MAGIC
# MAGIC
# MAGIC
# MAGIC class ToolCallingAgent(ResponsesAgent):
# MAGIC     """
# MAGIC     Class representing a tool-calling Agent
# MAGIC     """
# MAGIC
# MAGIC     def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
# MAGIC         """Initializes the ToolCallingAgent with tools."""
# MAGIC         self.llm_endpoint = llm_endpoint
# MAGIC         self.workspace_client = WorkspaceClient()
# MAGIC         self.model_serving_client: OpenAI = (
# MAGIC             self.workspace_client.serving_endpoints.get_open_ai_client()
# MAGIC         )
# MAGIC         # Internal message list holds conversation state in completion-message format
# MAGIC         self.messages: list[dict[str, Any]] = None
# MAGIC         self._tools_dict = {tool.name: tool for tool in tools}
# MAGIC
# MAGIC     def get_tool_specs(self) -> list[dict]:
# MAGIC         """Returns tool specifications in the format OpenAI expects."""
# MAGIC         return [tool_info.spec for tool_info in self._tools_dict.values()]
# MAGIC
# MAGIC     @mlflow.trace(span_type=SpanType.TOOL)
# MAGIC     def execute_tool(self, tool_name: str, args: dict) -> Any:
# MAGIC         """Executes the specified tool with the given arguments."""
# MAGIC         return self._tools_dict[tool_name].exec_fn(**args)
# MAGIC
# MAGIC     def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
# MAGIC         """Convert from a Responses API output item to  a list of ChatCompletion messages."""
# MAGIC         msg_type = message.get("type")
# MAGIC         if msg_type == "function_call":
# MAGIC             return [
# MAGIC                 {
# MAGIC                     "role": "assistant",
# MAGIC                     "content": "tool call",  # empty content is not supported by claude models
# MAGIC                     "tool_calls": [
# MAGIC                         {
# MAGIC                             "id": message["call_id"],
# MAGIC                             "type": "function",
# MAGIC                             "function": {
# MAGIC                                 "arguments": message["arguments"],
# MAGIC                                 "name": message["name"],
# MAGIC                             },
# MAGIC                         }
# MAGIC                     ],
# MAGIC                 }
# MAGIC             ]
# MAGIC         elif msg_type == "message" and isinstance(message.get("content"), list):
# MAGIC             return [
# MAGIC                 {"role": message["role"], "content": content["text"]}
# MAGIC                 for content in message["content"]
# MAGIC             ]
# MAGIC         elif msg_type == "reasoning":
# MAGIC             return [{"role": "assistant", "content": json.dumps(message["summary"])}]
# MAGIC         elif msg_type == "function_call_output":
# MAGIC             return [
# MAGIC                 {
# MAGIC                     "role": "tool",
# MAGIC                     "content": message["output"],
# MAGIC                     "tool_call_id": message["call_id"],
# MAGIC                 }
# MAGIC             ]
# MAGIC         compatible_keys = ["role", "content", "name", "tool_calls", "tool_call_id"]
# MAGIC         if not message.get("content") and message.get("tool_calls"):
# MAGIC             message["content"] = "tool call"
# MAGIC         filtered = {k: v for k, v in message.items() if k in compatible_keys}
# MAGIC         return [filtered] if filtered else []
# MAGIC
# MAGIC     def prep_msgs_for_llm(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
# MAGIC         """Filter out message fields that are not compatible with LLM message formats and convert from Responses API to ChatCompletion compatible"""
# MAGIC         chat_msgs = []
# MAGIC         for msg in messages:
# MAGIC             chat_msgs.extend(self._responses_to_cc(msg))
# MAGIC         return chat_msgs
# MAGIC
# MAGIC     def call_llm(self) -> Generator[dict[str, Any], None, None]:
# MAGIC         for chunk in self.model_serving_client.chat.completions.create(
# MAGIC             model=self.llm_endpoint,
# MAGIC             messages=self.prep_msgs_for_llm(self.messages),
# MAGIC             tools=self.get_tool_specs(),
# MAGIC             stream=True,
# MAGIC         ):
# MAGIC             yield chunk.to_dict()
# MAGIC
# MAGIC     def handle_tool_calls(
# MAGIC         self, tool_calls: list[dict[str, Any]]
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         """
# MAGIC         Execute tool calls, add them to the running message history, and return a ResponsesStreamEvent w/ tool output
# MAGIC         """
# MAGIC         for tool_call in tool_calls:
# MAGIC             function = tool_call["function"]
# MAGIC             args = json.loads(function["arguments"])
# MAGIC             # Cast tool result to a string, since not all tools return as tring
# MAGIC             result = str(self.execute_tool(tool_name=function["name"], args=args))
# MAGIC             self.messages.append(
# MAGIC                 {"role": "tool", "content": result, "tool_call_id": tool_call["id"]}
# MAGIC             )
# MAGIC             yield ResponsesAgentStreamEvent(
# MAGIC                 type="response.output_item.done",
# MAGIC                 item=self.create_function_call_output_item(
# MAGIC                     tool_call["id"],
# MAGIC                     result,
# MAGIC                 ),
# MAGIC             )
# MAGIC
# MAGIC     def call_and_run_tools(
# MAGIC         self,
# MAGIC         max_iter: int = 10,
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         for _ in range(max_iter):
# MAGIC             last_msg = self.messages[-1]
# MAGIC             if tool_calls := last_msg.get("tool_calls", None):
# MAGIC                 yield from self.handle_tool_calls(tool_calls)
# MAGIC             elif last_msg.get("role", None) == "assistant":
# MAGIC                 return
# MAGIC             else:
# MAGIC                 # aggregate the chat completions stream to add to internal state
# MAGIC                 llm_content = ""
# MAGIC                 tool_calls = []
# MAGIC                 msg_id = None
# MAGIC                 for chunk in self.call_llm():
# MAGIC                     delta = chunk["choices"][0]["delta"]
# MAGIC                     msg_id = chunk.get("id", None)
# MAGIC                     content = delta.get("content", None)
# MAGIC                     if tc := delta.get("tool_calls"):
# MAGIC                         if not tool_calls:  # only accomodate for single tool call right now
# MAGIC                             tool_calls = tc
# MAGIC                         else:
# MAGIC                             tool_calls[0]["function"]["arguments"] += tc[0]["function"]["arguments"]
# MAGIC                     elif content is not None:
# MAGIC                         llm_content += content
# MAGIC                         yield ResponsesAgentStreamEvent(
# MAGIC                             **self.create_text_delta(content, item_id=msg_id)
# MAGIC                         )
# MAGIC                 llm_output = {"role": "assistant", "content": llm_content, "tool_calls": tool_calls}
# MAGIC                 self.messages.append(llm_output)
# MAGIC
# MAGIC                 # yield an `output_item.done` `output_text` event that aggregates the stream
# MAGIC                 # this enables tracing and payload logging
# MAGIC                 if llm_output["content"]:
# MAGIC                     yield ResponsesAgentStreamEvent(
# MAGIC                         type="response.output_item.done",
# MAGIC                         item=self.create_text_output_item(
# MAGIC                             llm_output["content"], msg_id
# MAGIC                         ),
# MAGIC                     )
# MAGIC                 # yield an `output_item.done` `function_call` event for each tool call
# MAGIC                 if tool_calls := llm_output.get("tool_calls", None):
# MAGIC                     for tool_call in tool_calls:
# MAGIC                         yield ResponsesAgentStreamEvent(
# MAGIC                             type="response.output_item.done",
# MAGIC                             item=self.create_function_call_item(
# MAGIC                                 str(uuid4()),
# MAGIC                                 tool_call["id"],
# MAGIC                                 tool_call["function"]["name"],
# MAGIC                                 tool_call["function"]["arguments"],
# MAGIC                             ),
# MAGIC                         )
# MAGIC
# MAGIC         yield ResponsesAgentStreamEvent(
# MAGIC             type="response.output_item.done",
# MAGIC             item=self.create_text_output_item("Max iterations reached. Stopping.", str(uuid4())),
# MAGIC         )
# MAGIC
# MAGIC     def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
# MAGIC         outputs = [
# MAGIC             event.item
# MAGIC             for event in self.predict_stream(request)
# MAGIC             if event.type == "response.output_item.done"
# MAGIC         ]
# MAGIC         return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self, request: ResponsesAgentRequest
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         self.messages = self.prep_msgs_for_llm([i.model_dump() for i in request.input])
# MAGIC         if SYSTEM_PROMPT:
# MAGIC             self.messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})
# MAGIC         yield from self.call_and_run_tools()
# MAGIC
# MAGIC
# MAGIC # Log the model using MLflow
# MAGIC mlflow.openai.autolog()
# MAGIC AGENT = ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
# MAGIC mlflow.models.set_model(AGENT)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since we manually traced methods within `ResponsesAgent`, you can view the trace for each step the agent takes, with any LLM calls made via the OpenAI SDK automatically traced by autologging.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

from agent import AGENT

AGENT.predict({"input": [{"role": "user", "content": "what is 4*3 in python"}]})

# COMMAND ----------

for chunk in AGENT.predict_stream(
    {"input": [{"role": "user", "content": "What is 4*3 in Python?"}]}
):
    print(chunk.model_dump(exclude_none=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Determine Databricks resources to specify for automatic auth passthrough at deployment time
# MAGIC - **TODO**: If your Unity Catalog Function queries a [vector search index](https://docs.databricks.com/generative-ai/agent-framework/unstructured-retrieval-tools.html) or leverages [external functions](https://docs.databricks.com/generative-ai/agent-framework/external-connection-tools.html), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See [docs](https://docs.databricks.com/generative-ai/agent-framework/log-agent.html#specify-resources-for-automatic-authentication-passthrough) for more details.
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import UC_TOOL_NAMES, VECTOR_SEARCH_TOOLS, LLM_ENDPOINT_NAME
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from pkg_resources import get_distribution

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]
for tool in VECTOR_SEARCH_TOOLS:
    resources.extend(tool.resources)
for tool_name in UC_TOOL_NAMES:
    # TODO: If the UC function includes dependencies like external connection or vector search, please include them manually.
    # See the TODO in the markdown above for more information.    
    resources.append(DatabricksFunction(function_name=tool_name))

input_example = {
    "input": [
        {
            "role": "user",
            "content": "What is an LLM agent?"
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",
        input_example=input_example,
        pip_requirements=[
            "databricks-openai",
            "backoff",
            f"databricks-connect=={get_distribution('databricks-connect').version}",
        ],
        resources=resources,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/mlflow3/genai/eval-monitor)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.
# MAGIC
# MAGIC Evaluate your agent with one of our [predefined LLM scorers](https://docs.databricks.com/mlflow3/genai/eval-monitor/predefined-judge-scorers), or try adding [custom metrics](https://docs.databricks.com/mlflow3/genai/eval-monitor/custom-scorers).

# COMMAND ----------

import mlflow
from mlflow.genai.scorers import RelevanceToQuery, Safety, RetrievalRelevance, RetrievalGroundedness

eval_dataset = [
    {
        "inputs": {
            "input": [
                {
                    "role": "user",
                    "content": "what did nicolas.pelaez@example.com order?"
                }
            ]
        },
        "expected_response": """
        I've retrieved the information for nicolas.pelaez@example.com. Based on the data, this customer has had multiple interactions with customer service over the last 12 months:
193 returns related to Technical Support issues
205 returns related to Account Management issues
213 returns related to Billing issues
187 returns related to Feedback
23 returns specifically categorized as Returns
202 returns related to Product Inquiry
This shows a total of 1,023 returns across various categories in the last 12 months. The data is current as of today (August 29, 2025).
Would you like more specific information about any of these categories or would you like to know about our return policy?
        """
    }
]

eval_results = mlflow.genai.evaluate(
    data=eval_dataset,
    predict_fn=lambda input: AGENT.predict({"input": input}),
    scorers=[RelevanceToQuery(), Safety()], # add more scorers here if they're applicable
)

# Review the evaluation results in the MLfLow UI (see console output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform pre-deployment validation of the agent
# MAGIC Before registering and deploying the agent, we perform pre-deployment checks via the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See [documentation](https://docs.databricks.com/machine-learning/model-serving/model-serving-debug.html#validate-inputs) for details

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/agent",
    input_data={"input": [{"role": "user", "content": "what did nicolas.pelaez@example.com order?!"}]},
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = "dbacademy"
schema = {user_schema}
model_name = "demo-agent-model"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents
agents.deploy(
    UC_MODEL_NAME, 
    uc_registered_model_info.version, 
    tags = {"endpointSource": "playground"},
    scale_to_zero_enabled=True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See [docs](https://docs.databricks.com/generative-ai/deploy-agent.html) for details
