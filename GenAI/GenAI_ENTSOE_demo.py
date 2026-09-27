# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GenAI on Databricks with Mosaic AI - Lab 11
# MAGIC
# MAGIC RAG + Agent + Model Serving over the **governed gold layer** of the datacenter energy-cost
# MAGIC project (Labs 3-9). The agent has two skills:
# MAGIC
# MAGIC 1. **SQL tool** - a Unity Catalog function over `gold.consumption_hourly` (numbers).
# MAGIC 2. **RAG retriever** - a Vector Search index over the project `README` files (documentation) + Entsoe Handboo.
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

# MAGIC %pip install -U -qqq databricks-langchain databricks-vectorsearch databricks-agents "mlflow[databricks]" "unitycatalog-ai[databricks]" "unitycatalog-langchain[databricks]" "langgraph-prebuilt==1.0.8"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.dropdown("target_env", "dev", ["dev", "prod"], "Target environment")
TARGET_ENV = dbutils.widgets.get("target_env")

ENV_CONFIG = {
    "dev": {   # your Free Edition / Free Trial
        "catalog": "workspace",
        "gold_schema": "gold",
        "docs_path": "/Workspace/Repos/gabrielajaniszewska@translite.pl/Databricks_Academy/GenAI/docs",
        "pdf_volume_path": "/Volumes/workspace/gold/docs_volume/MoP_Ref2_DDD_v3r4.pdf",
    },
    "prod": {  # shared Azure workspace (profile dbr_dev_trial, catalog is still dbr_dev)
        "catalog": "dbr_dev",
        "gold_schema": "gabrielajaniszews786_gold",
        "docs_path": "/Workspace/Repos/gabrielajaniszewska@translite.pl/Databricks_Academy/GenAI/docs",
        "pdf_volume_path": "/Volumes/dbr_dev/gabrielajaniszews786_gold/docs_volume/MoP_Ref2_DDD_v3r4.pdf",
    },
}

cfg = ENV_CONFIG[TARGET_ENV]
CATALOG      = cfg["catalog"]
GOLD_SCHEMA  = cfg["gold_schema"]
DOCS_PATH    = cfg["docs_path"]
PDF_VOLUME_PATH = cfg["pdf_volume_path"]

FACT_TABLE   = f"{CATALOG}.{GOLD_SCHEMA}.consumption_hourly"
DIM_DATE     = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"
DOCS_TABLE   = f"{CATALOG}.{GOLD_SCHEMA}.project_docs"
VS_INDEX     = f"{CATALOG}.{GOLD_SCHEMA}.project_docs_index"
UC_MODEL     = f"{CATALOG}.{GOLD_SCHEMA}.entsoe_support_agent"

VS_ENDPOINT  = "entsoe_vs" if TARGET_ENV == "dev" else "prod-retail-rag-endpoint"

LLM_ENDPOINT       = "databricks-meta-llama-3-3-70b-instruct"
EMBEDDING_ENDPOINT = "databricks-gte-large-en"

print(f"Running against: {TARGET_ENV} | catalog={CATALOG} | schema={GOLD_SCHEMA}")
print(f"DOCS_TABLE = {DOCS_TABLE}")

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
records += chunk_pdf(PDF_VOLUME_PATH)   # let a real failure here surface immediately

from collections import Counter
counts = Counter(r.doc_type for r in records)
assert counts.get("internal_docs", 0) > 0, f"No README chunks found at {DOCS_PATH}."
assert counts.get("external_reference", 0) > 0, f"No PDF chunks found at {PDF_VOLUME_PATH}."

print(f"CATALOG={CATALOG}, DOCS_TABLE={DOCS_TABLE}")
print(f"Corpus ready: {dict(counts)}")

docs_df = spark.createDataFrame(records).withColumn("id", F.monotonically_increasing_id())
(docs_df.write.mode("overwrite")
        .option("delta.enableChangeDataFeed", "true")
        .option("mergeSchema", "true")
        .saveAsTable(DOCS_TABLE))
spark.sql(f"ALTER TABLE {DOCS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"{docs_df.count()} chunks written to {DOCS_TABLE}")
display(spark.table(DOCS_TABLE).select("doc_name", "section", "doc_type"))

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# Create the endpoint once (skip/ignore if it already exists).
try:
    index = vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=DOCS_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
        columns_to_sync=["id", "doc_name", "section", "content", "doc_type"],
    )
    print(f"Created new index: {VS_INDEX}")
except Exception as e:
    if "already exists" in str(e).lower():
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
        index.sync()
        print(f"Index already existed - triggered a re-sync from {DOCS_TABLE} instead.")
    else:
        raise

# COMMAND ----------

import time
for i in range(30):
    status = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX).describe()["status"]
    print(f"[{i*20}s] state={status['detailed_state']}, ready={status['ready']}")
    if status["ready"]:
        print("Index is online.")
        break
    time.sleep(20)
else:
    print("Still not ready after 10 minutes - worth checking the endpoint page in the UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Assemble the agent
# MAGIC LLM + [SQL tools, RAG retriever] wired into a LangGraph ReAct agent. Run this live - it is fast.

# COMMAND ----------

# DBTITLE 1,Cell 14
# Section 3: Assemble the agent
from databricks_langchain import ChatDatabricks, UCFunctionToolkit, VectorSearchRetrieverTool
from langchain.agents import create_agent

llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)

uc_tools = UCFunctionToolkit(function_names=[
    f"{CATALOG}.{GOLD_SCHEMA}.get_energy_cost",
    f"{CATALOG}.{GOLD_SCHEMA}.list_bidding_zones",
]).tools

internal_docs_tool = VectorSearchRetrieverTool(
    index_name=VS_INDEX,
    num_results=3,
    columns=["doc_name", "section", "content", "doc_type"],
    filters={"doc_type": "internal_docs"},
    tool_name="search_internal_docs",
    tool_description="Search the project's own README documentation - how the pipeline, gold layer, "
                      "governance, tests and alerts were built in THIS project.",
)

external_reference_tool = VectorSearchRetrieverTool(
    index_name=VS_INDEX,
    num_results=3,
    columns=["doc_name", "section", "content", "doc_type"],
    filters={"doc_type": "external_reference"},
    tool_name="search_entsoe_glossary",
    tool_description="Search the official ENTSO-E reference documentation - definitions and specifications "
                      "such as bidding zones, aFRR/mFRR, imbalance settlement, and other domain terminology "
                      "NOT specific to this project's implementation.",
)

tools = uc_tools + [internal_docs_tool, external_reference_tool]

SYSTEM_PROMPT = (
    "You are the ENTSO-E project assistant. "
    "For questions about numbers (cost, consumption, PUE, zones), call the SQL tools. "
    "For questions about how something was built or configured, call search_project_docs. "
    "Always name the bidding zone and date range you used. If a zone is unknown, call list_bidding_zones first."
)

agent = create_agent(llm, tools, system_prompt=SYSTEM_PROMPT)

# COMMAND ----------

# MAGIC %md Live question 1 - numbers (hits the SQL tool + governed table):

# COMMAND ----------

for step in agent.stream(
    {"messages": [{"role": "user",
                   "content": "What was the total energy cost in the PL bidding zone in September 2026, and the average PUE?"}]},
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
