# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GenAI on Databricks with Mosaic AI - live demo on the ENTSO-E project
# MAGIC
# MAGIC RAG + Agent + Model Serving over the **governed gold layer** of the datacenter energy-cost
# MAGIC project (Labs 3-7). The agent has two skills:
# MAGIC
# MAGIC 1. **SQL tool** - a Unity Catalog function over `gold.consumption_hourly` (numbers).
# MAGIC 2. **RAG retriever** - a Vector Search index over the project `README` files (documentation).
# MAGIC
# MAGIC The SQL tool reads the same table that already carries row-level security (`regional_filter`) and a column mask (`site_id_mask`), so the agent inherits
# MAGIC them with **zero extra access-control code**.
# MAGIC
# MAGIC > No-code counterpart already exists: your **Genie space** over the gold layer is the low-code agent.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config
# MAGIC

# COMMAND ----------

# MAGIC %pip install -U -qqq databricks-langchain databricks-vectorsearch databricks-agents "mlflow[databricks]" "unitycatalog-ai[databricks]" "unitycatalog-langchain[databricks]" pypdf
# MAGIC %restart_python

# COMMAND ----------

# --- adapt these to the workspace you demo in -------------------------------
CATALOG      = "workspace"     # dev target; use "dbr_dev" for prod
GOLD_SCHEMA  = "gold"
FACT_TABLE   = f"{CATALOG}.{GOLD_SCHEMA}.consumption_hourly"
DIM_DATE     = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"

# GenAI resources
LLM_ENDPOINT       = "databricks-meta-llama-3-3-70b-instruct"  # or databricks-claude-sonnet-5, etc.
EMBEDDING_ENDPOINT = "databricks-gte-large-en"

# RAG corpus + index
DOCS_TABLE   = f"{CATALOG}.{GOLD_SCHEMA}.project_docs"
VS_ENDPOINT  = "entsoe_vs"                                   # Free Edition allows exactly 1
VS_INDEX     = f"{CATALOG}.{GOLD_SCHEMA}.project_docs_index"
DOCS_PATH    = "/Workspace/Repos/gabrielajaniszewska@translite.pl/Databricks_Academy/GenAI/docs"  # folder that holds the README.md files
PDF_VOLUME_PATH = "/Volumes/dbr_dev/gabrielajaniszews786_gold/docs_volume/MoP_Ref2_DDD_v3r4.pdf"  # ENTSO-E reference PDF (external_reference doc_type)

# Where to register + deploy the agent
UC_MODEL     = f"{CATALOG}.{GOLD_SCHEMA}.entsoe_support_agent"

print("Fact table:", FACT_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The SQL tool  (Agents slide)
# MAGIC A Unity Catalog function is a governed, reusable tool. It runs with the **caller's** privileges, so any
# MAGIC row filter / column mask on `consumption_hourly` applies automatically.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{GOLD_SCHEMA}.get_energy_cost(
  zone       STRING COMMENT 'Bidding zone code, for example PL or DE_LU',
  start_date DATE   COMMENT 'Inclusive start date (yyyy-MM-dd)',
  end_date   DATE   COMMENT 'Inclusive end date (yyyy-MM-dd)'
)
RETURNS TABLE(bidding_zone STRING, total_cost DOUBLE, avg_pue DOUBLE, hours BIGINT)
COMMENT 'Total energy cost and average PUE for a bidding zone over a date range, from the gold consumption_hourly fact.'
RETURN
  SELECT bidding_zone,
         ROUND(SUM(cost_per_hour), 2) AS total_cost,
         ROUND(AVG(avg_pue), 3)       AS avg_pue,
         COUNT(*)                     AS hours
  FROM {FACT_TABLE}
  WHERE bidding_zone = get_energy_cost.zone
    AND date BETWEEN get_energy_cost.start_date AND get_energy_cost.end_date
  GROUP BY bidding_zone
""")

# A second tool so the agent can discover valid zones instead of guessing.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{GOLD_SCHEMA}.list_bidding_zones()
RETURNS TABLE(bidding_zone STRING, sites BIGINT)
COMMENT 'Lists the bidding zones available in the gold layer and how many sites each has.'
RETURN
  SELECT bidding_zone, COUNT(DISTINCT site_id) AS sites
  FROM {FACT_TABLE}
  GROUP BY bidding_zone
""")
print("UC functions created.")

# COMMAND ----------

# MAGIC %md Smoke-test the function with plain SQL

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.list_bidding_zones()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The RAG retriever  (RAG slide)
# MAGIC Corpus = the project `README` files. We chunk them by markdown section, land them in a Delta table with
# MAGIC Change Data Feed on, then build a **Delta Sync** Vector Search index with managed embeddings
# MAGIC (the only index type Free Edition supports).

# COMMAND ----------

import os, glob, re
from pyspark.sql import Row, functions as F

def chunk_markdown(path):
    """One row per '##' section; keeps the section heading with its text."""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    doc = os.path.splitext(os.path.basename(path))[0].replace("README_", "").title()
    parts = re.split(r"\n(?=#{1,3}\s)", text)
    rows = []
    for i, part in enumerate(parts):
        part = part.strip()
        if len(part) < 40:
            continue
        heading = part.splitlines()[0].lstrip("# ").strip()
        rows.append(Row(doc_name=doc, section=heading[:200], content=part, doc_type="internal_docs"))
    return rows

def chunk_pdf(path, max_chars=2000):
    """One row per PDF page, split further if a page runs long; same shape as chunk_markdown's rows."""
    from pypdf import PdfReader

    doc = os.path.splitext(os.path.basename(path))[0]
    reader = PdfReader(path)
    rows = []
    for page_num, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").strip()
        if len(text) < 40:
            continue
        pieces = [text[i:i + max_chars] for i in range(0, len(text), max_chars)]
        for j, piece in enumerate(pieces, start=1):
            section = f"Page {page_num}" + (f" (part {j})" if len(pieces) > 1 else "")
            rows.append(Row(doc_name=doc, section=section, content=piece, doc_type="external_reference"))
    return rows

paths = glob.glob(os.path.join(DOCS_PATH, "README*.md"))
records = [r for p in paths for r in chunk_markdown(p)]

if os.path.exists(PDF_VOLUME_PATH):
    try:
        records += chunk_pdf(PDF_VOLUME_PATH)
    except Exception as e:
        print(f"WARNING: failed to chunk PDF at {PDF_VOLUME_PATH} - {e}")
else:
    print(f"WARNING: no PDF found at {PDF_VOLUME_PATH} - skipping external_reference chunks.")

# Fallback so the cell never hard-fails during a live demo if the path is off.
if not records:
    records = [Row(doc_name="Lab6",
                   section="Row-Level Security",
                   content="RLS in the gold layer uses regional_filter(bidding_zone): members of Poland AND "
                           "admins see only PL rows. Applied with ALTER MATERIALIZED VIEW consumption_hourly "
                           "SET ROW FILTER regional_filter ON (bidding_zone).",
                   doc_type="internal_docs")]
    print("WARNING: no README files found at DOCS_PATH - using an inline fallback chunk.")

docs_df = spark.createDataFrame(records).withColumn("id", F.monotonically_increasing_id())
(docs_df.write.mode("overwrite")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(DOCS_TABLE))
spark.sql(f"ALTER TABLE {DOCS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"{docs_df.count()} chunks written to {DOCS_TABLE}")
display(spark.table(DOCS_TABLE).select("doc_name", "section", "doc_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC **PRE-RUN before the talk.** Creating the endpoint and syncing the index takes a few minutes.
# MAGIC Run this the evening before, not live.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# Create the endpoint once (skip/ignore if it already exists).
try:
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")
except Exception as e:
    print("Endpoint note:", e)

index = vsc.create_delta_sync_index_and_wait(
    endpoint_name=VS_ENDPOINT,
    index_name=VS_INDEX,
    source_table_name=DOCS_TABLE,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column="content",
    embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
    columns=["id", "doc_name", "section", "content", "doc_type"],  # doc_type synced so retrieval can filter on it
)
print("Index online:", VS_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Assemble the agent
# MAGIC LLM + [SQL tools, RAG retriever] wired into a LangGraph ReAct agent. Run this live - it is fast.

# COMMAND ----------

# DBTITLE 1,Cell 14
# NOTE: Using a simplified agent implementation to avoid langgraph version incompatibility
# The environment has langgraph 1.0.10 + langgraph-prebuilt 1.0.13 which are incompatible
# This custom implementation provides the same interface without requiring langgraph.prebuilt

from databricks_langchain import ChatDatabricks, UCFunctionToolkit, VectorSearchRetrieverTool
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage
from typing import Dict, List, Any

llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)

uc_tools = UCFunctionToolkit(function_names=[
    f"{CATALOG}.{GOLD_SCHEMA}.get_energy_cost",
    f"{CATALOG}.{GOLD_SCHEMA}.list_bidding_zones",
]).tools

rag_tool = VectorSearchRetrieverTool(
    index_name=VS_INDEX,
    num_results=3,
    columns=["doc_name", "section", "content", "doc_type"],
    tool_name="search_project_docs",
    tool_description="Search the project documentation: internal_docs (the project README files - how the "
                      "pipeline, gold layer, governance, tests and alerts were built) and external_reference "
                      "(the ENTSO-E reference PDF). Each result's doc_type tells you which one it came from.",
)

tools = uc_tools + [rag_tool]

SYSTEM_PROMPT = (
    "You are the ENTSO-E project assistant. "
    "For questions about numbers (cost, consumption, PUE, zones), call the SQL tools. "
    "For questions about how something was built or configured, call search_project_docs. "
    "Always name the bidding zone and date range you used. If a zone is unknown, call list_bidding_zones first."
)

# Custom agent class that mimics langgraph's create_react_agent interface
class SimpleReActAgent:
    def __init__(self, llm, tools, system_prompt):
        self.llm = llm.bind_tools(tools)  # Enable tool calling
        self.tools_by_name = {t.name: t for t in tools}
        self.system_prompt = system_prompt
    
    def stream(self, input_dict, stream_mode="values"):
        """Stream agent responses, yielding message states."""
        # Convert input messages to LangChain format
        messages = [SystemMessage(content=self.system_prompt)]
        for msg in input_dict["messages"]:
            if isinstance(msg, dict):
                role = msg.get("role")
                content = msg.get("content", "")
                if role == "user":
                    messages.append(HumanMessage(content=content))
                elif role == "assistant":
                    messages.append(AIMessage(content=content))
            else:
                messages.append(msg)
        
        # Yield initial state with user message
        yield {"messages": messages}
        
        max_iterations = 10
        for _ in range(max_iterations):
            # Get LLM response with tool calls
            response = self.llm.invoke(messages)
            messages.append(response)
            yield {"messages": messages}
            
            # Check if we're done (no tool calls)
            if not response.tool_calls:
                break
            
            # Execute tool calls
            for tool_call in response.tool_calls:
                tool = self.tools_by_name.get(tool_call["name"])
                if tool:
                    try:
                        result = tool.invoke(tool_call["args"])
                        tool_msg = ToolMessage(
                            content=str(result),
                            tool_call_id=tool_call.get("id", ""),
                            name=tool_call["name"]
                        )
                    except Exception as e:
                        tool_msg = ToolMessage(
                            content=f"Error: {str(e)}",
                            tool_call_id=tool_call.get("id", ""),
                            name=tool_call["name"]
                        )
                    messages.append(tool_msg)
                    yield {"messages": messages}

agent = SimpleReActAgent(llm, tools, SYSTEM_PROMPT)

# COMMAND ----------

# MAGIC %md Live question 1 - numbers (hits the SQL tool + governed table):

# COMMAND ----------

for step in agent.stream(
    {"messages": [{"role": "user",
                   "content": "What was the total energy cost in the PL bidding zone in August 2026, and the average PUE?"}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()

# COMMAND ----------

# MAGIC %md Live question 2 - documentation (hits the RAG retriever):

# COMMAND ----------

for step in agent.stream(
    {"messages": [{"role": "user",
                   "content": "How was row-level security implemented in the gold layer of this project?"}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Governance punchline  (Governance slide)
# MAGIC The SQL tool reads `consumption_hourly`, which already has a row filter and a column mask.
# MAGIC The agent inherits them - no `if/else` in the agent code.

# COMMAND ----------

# Shows the ROW FILTER (regional_filter) and COLUMN MASK (site_id_mask) on the very table the tool queries.
display(spark.sql(f"DESCRIBE EXTENDED {FACT_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Log, register to Unity Catalog, deploy to Model Serving  (Model Serving slide)
# MAGIC **PRE-RUN before the talk** - deployment takes several minutes. During the talk, show the endpoint
# MAGIC that is already live (or the AI Playground), do not deploy on stage.
# MAGIC
# MAGIC The recommended path wraps the agent in MLflow's `ResponsesAgent` in a separate `agent.py`, logs it
# MAGIC **as code** with its resources declared (so serving gets automatic auth), registers it to UC, then
# MAGIC `agents.deploy()`. Skeleton below - adapt to your workspace.

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksServingEndpoint, DatabricksFunction, DatabricksVectorSearchIndex,
)
from databricks import agents

# Declaring resources lets Model Serving mint short-lived credentials for exactly these objects.
resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksServingEndpoint(endpoint_name=EMBEDDING_ENDPOINT),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
    DatabricksFunction(function_name=f"{CATALOG}.{GOLD_SCHEMA}.get_energy_cost"),
    DatabricksFunction(function_name=f"{CATALOG}.{GOLD_SCHEMA}.list_bidding_zones"),
]

with mlflow.start_run(run_name="entsoe_support_agent"):
    logged = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",   # <- a ResponsesAgent wrapper around the graph above; see Databricks "author agent" docs
        resources=resources,
        pip_requirements=["databricks-langchain", "databricks-vectorsearch", "langgraph", "mlflow"],
    )

uc_model = mlflow.register_model(model_uri=logged.model_uri, name=UC_MODEL)
agents.deploy(UC_MODEL, uc_model.version, scale_to_zero=True)  # CPU + scale-to-zero on Free Edition
print("Deployed:", UC_MODEL, "v", uc_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evaluate  (Evaluation slide)
# MAGIC A tiny golden set (numbers + docs). MLflow 3 runs built-in LLM judges so you can gate prompt changes
# MAGIC in CI instead of eyeballing. Run live - it is quick and visual.

# COMMAND ----------

import mlflow
from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety, RetrievalGroundedness

def predict_fn(messages):
    result = agent.invoke({"messages": messages})
    return result["messages"][-1].content

eval_data = [
    {"inputs": {"messages": [{"role": "user", "content": "Which bidding zones are in the gold layer?"}]},
     "expectations": {"expected_facts": ["PL"]}},
    {"inputs": {"messages": [{"role": "user", "content": "What was the total energy cost in PL in August 2026?"}]},
     "expectations": {"expected_facts": ["a numeric total cost for PL"]}},
    {"inputs": {"messages": [{"role": "user", "content": "How is dim_date generated in this project?"}]},
     "expectations": {"expected_facts": ["generated from a date range, not scanned from the fact"]}},
    {"inputs": {"messages": [{"role": "user", "content": "What alert monitors data volume, and at what threshold?"}]},
     "expectations": {"expected_facts": ["fewer than 192 rows per day (8 sites x 24 hours)"]}},
]

results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=[Correctness(), RelevanceToQuery(), Safety(), RetrievalGroundedness()],
)
print("Open the MLflow run to see per-question judge scores.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap - one project, the whole title
# MAGIC - **RAG**  -> Vector Search over the project READMEs
# MAGIC - **Agents** -> UC-function SQL tools + retriever in one LangGraph agent
# MAGIC - **Governance** -> RLS + CLS inherited from `consumption_hourly`, no extra code
# MAGIC - **Model Serving** -> registered in UC, deployed as a scale-to-zero endpoint
# MAGIC - **Evaluation** -> MLflow 3 LLM judges as a quality gate
