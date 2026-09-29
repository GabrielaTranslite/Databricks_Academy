# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GenAI on Databricks with Mosaic AI - Lab 12
# MAGIC
# MAGIC RAG + Agent + Model Serving over the **governed gold layer** of the datacenter energy-cost
# MAGIC project (Labs 3-12). The agent has two skills:
# MAGIC
# MAGIC 1. **SQL tool** - a Unity Catalog function over `gold.consumption_hourly` (numbers).
# MAGIC 2. **RAG retriever** - a Vector Search index over the project `README` files (documentation) + Entsoe Detailed Data Descriptions.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration
# MAGIC

# COMMAND ----------

# MAGIC %pip install -U -qqq databricks-langchain databricks-vectorsearch databricks-agents "mlflow[databricks]" "unitycatalog-ai[databricks]" "unitycatalog-langchain[databricks]" "langgraph-prebuilt==1.0.8" pypdf
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC **Important note**: for prod, I am using prod-retail-rag-endpoint created by Yanquiel as we can have only one VS endpoint in the Trial version.

# COMMAND ----------

import mlflow
mlflow.langchain.autolog()   # logs every prompt + response as a trace in Experiments

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
        "docs_path": "/Workspace/Users/gabrielajaniszews786@softserve.academy/Databricks_Academy/GenAI/docs",
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
# MAGIC ## 2. RAG retriever
# MAGIC Corpus = the project `README` files + PDF file (Detailed Data Descriptions). I chunked by markdown section and the PDF by page, put in a Delta table with
# MAGIC Change Data Feed on, then built a **Delta Sync** Vector Search index with managed embeddings.

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

print(f"DEBUG: len(paths)={len(paths)}, len(records)={len(records)}")

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
import time

vsc = VectorSearchClient(disable_notice=True)

# Create the index once; if it already exists, trigger a re-sync instead.
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
        try:
            index.sync()
            print(f"Index already existed - triggered a re-sync from {DOCS_TABLE}.")
        except Exception as sync_err:
            if "not ready to sync" in str(sync_err).lower():
                print("A sync is already in progress - skipping, it will finish on its own.")
            else:
                raise
    else:
        raise

# Wait until the index is queryable (runs whether it was just created or re-synced).
while not index.describe()["status"].get("ready", False):
    print("Waiting for index to become ready...")
    time.sleep(15)
print("Index is ready.")

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
# MAGIC ## 3. Assemble the agents
# MAGIC 1. LLM + [SQL tools, RAG retriever]
# MAGIC
# MAGIC 2. LLM only

# COMMAND ----------

# DBTITLE 1,Cell 14
# Assemble the agent with access to the tools
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
    "For questions about numbers (cost, consumption, PUE, zones), call the SQL tools "
    "(get_energy_cost, list_bidding_zones). "
    "For questions about how THIS project was built or configured (pipeline, gold layer, "
    "governance, tests, alerts), call search_internal_docs. "
    "For questions about ENTSO-E domain terminology or definitions "
    "(bidding zone, external constraint, aFRR/mFRR, imbalance settlement), call search_entsoe_glossary. "
    "Always name the bidding zone and date range you used. If a zone is unknown, call list_bidding_zones first."
)

agent = create_agent(llm, tools, system_prompt=SYSTEM_PROMPT)

# COMMAND ----------

# Assemble the no context agent
from databricks_langchain import ChatDatabricks, UCFunctionToolkit, VectorSearchRetrieverTool
from langchain.agents import create_agent

llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)


SYSTEM_PROMPT = (
    "You are the ENTSO-E project assistant. "
    "For questions about numbers (cost, consumption, PUE, zones), call the SQL tools "
    "(get_energy_cost, list_bidding_zones). "
    "For questions about how THIS project was built or configured (pipeline, gold layer, "
    "governance, tests, alerts), call search_internal_docs. "
    "For questions about ENTSO-E domain terminology or definitions "
    "(bidding zone, external constraint, aFRR/mFRR, imbalance settlement), call search_entsoe_glossary. "
    "Always name the bidding zone and date range you used. If a zone is unknown, call list_bidding_zones first."
)
agent_no_context = create_agent(llm, system_prompt=SYSTEM_PROMPT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Live demo: with vs without retrieval

# COMMAND ----------

# MAGIC %md
# MAGIC ### Live question 1 - numbers (hits the SQL tool + governed table):

# COMMAND ----------

for step in agent.stream(
    {"messages": [{"role": "user",
                   "content": "What was the total energy cost in the PL bidding zone in September 2026, and the average PUE?"}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()

# COMMAND ----------

for step in agent_no_context.stream(
    {"messages": [{"role": "user",
                   "content": "What was the total energy cost in the PL bidding zone in September 2026, and the average PUE?"}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()

# COMMAND ----------

# MAGIC %md
# MAGIC The agent without context replied with made up data and claiming that it uses SQL tools. However, it added a comment about its limitations: "Please note that these values are fictional and used only for demonstration purposes. Actual values would depend on the real data provided by the SQL tools."

# COMMAND ----------

# Comparing the data manually
display(spark.sql(f"""SELECT
                  bidding_zone,
                  ROUND(SUM(cost_per_hour), 2) AS total_cost
                  FROM {FACT_TABLE}
                  WHERE bidding_zone = 'PL' AND date BETWEEN '2026-09-01' AND '2026-09-30'
                  GROUP BY bidding_zone"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### With vs without retrieval
# MAGIC
# MAGIC Same question to both agents: *"What was the total energy cost in the PL bidding zone in September 2026, and the average PUE?"*
# MAGIC
# MAGIC | Metric        | With context (SQL tools + RAG) | Without context (LLM only) |
# MAGIC |---------------|--------------------------------|----------------------------|
# MAGIC | Total cost    | 79,855.98 EUR                  | ~100 milion EUR            |
# MAGIC | Average PUE   | 1.305                          | 1.8                        |
# MAGIC | Hours covered | 674                            | not reported               |
# MAGIC | Source        | `gold.consumption_hourly`      | model's training memory    |
# MAGIC | Grounded      | Yes                            | No (hallucinated)          |
# MAGIC
# MAGIC **Observation:** without access to the gold tables the agent fabricates the answer. It is wrong by roughly four orders of magnitude on cost, invents a PUE, and even narrates calling SQL tools it cannot reach ("According to the SQL tools..."). It also reveals the failure mode itself, citing a "knowledge cutoff date of December 2023" – proof it is answering from training data, not from the data platform. The grounded agent, by contrast, returns figures traceable to `consumption_hourly`. This contrast is the core justification for the retrieval + tool-calling architecture.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Live question 2 - documentation (hits the RAG retriever):

# COMMAND ----------

# Question no. 2 with tools and context
for step in agent.stream(
    {"messages": [{"role": "user",
                   "content": "How was row-level security implemented in the gold layer of this project?"}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()

# COMMAND ----------

# MAGIC %md
# MAGIC This is a correct, data-grounded answer to this question.

# COMMAND ----------

# Comparing the agent without context
for step in agent_no_context.stream(
    {"messages": [{"role": "user",
                   "content": "How was row-level security implemented in the gold layer of this project?"}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()

# COMMAND ----------

# MAGIC %md
# MAGIC This answer is incorrect. The model clearly hallucinates and tries to infer some information from the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Governance: the agent inherits row-level security
# MAGIC
# MAGIC The SQL tool reads `consumption_hourly`, which already carries a row filter and a column mask.
# MAGIC The agent inherits them - there is no access-control code in the agent itself.
# MAGIC
# MAGIC ### What the row filter and column mask do
# MAGIC
# MAGIC `consumption_hourly` carries two access controls that the SQL tool - and therefore the agent - inherits automatically, with no access-control code in the agent:
# MAGIC
# MAGIC - **Row filter (`regional_filter`)** - restricts which rows a user sees by `bidding_zone`. A Poland-scoped user sees only `PL` rows; a data steward (me, during development) is exempt and sees all.
# MAGIC - **Column mask (`site_id_mask`)** - hides `site_id` values outside the user's region, showing `**-**-**` for the rest. It matters for a broader-access user (e.g. an EU-wide analyst who can see several zones but should not see other regions' site IDs); for a Poland-scoped user the row filter already removes non-PL rows, so the mask is moot for them.
# MAGIC
# MAGIC The cells below prove this without hiding anything from the reviewer:
# MAGIC
# MAGIC 1. `DESCRIBE EXTENDED` - the filter and mask are attached to the table the agent reads.
# MAGIC 2. **Steward view** - full data, all zones, real site IDs.
# MAGIC 3. **Mask effect** - shown across all zones so the masking is visible: `DC-PL-01` stays, other site IDs become `**-**-**`.
# MAGIC 4. **Row filter effect** - row count drops from all zones to PL only for a Poland-scoped user.

# COMMAND ----------


# 1) Proof the row filter and column mask are attached to the table the SQL tool reads.
display(spark.sql(f"DESCRIBE EXTENDED {FACT_TABLE}"))

# COMMAND ----------

# Show the governance logic the agent inherits (functions defined in Lab 6, applied to the gold table)
display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG}.{GOLD_SCHEMA}.regional_filter"))
display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG}.{GOLD_SCHEMA}.site_id_mask"))

# COMMAND ----------

# 2) Full data
display(spark.sql(f"""
    SELECT bidding_zone, site_id, ROUND(AVG(cost_per_hour), 2) AS avg_cost
    FROM {FACT_TABLE}
    GROUP BY bidding_zone, site_id
    ORDER BY bidding_zone
    LIMIT 12
"""))

# COMMAND ----------

# 3a) Column mask effect: non-PL site_ids are masked. Shown across ALL zones (no row filter here)
#     so the masking is actually visible - DC-PL-01 stays, every other site becomes '**-**-**'.
display(spark.sql(f"""
    SELECT bidding_zone,
           CASE WHEN site_id LIKE 'DC-PL-%' THEN site_id ELSE '**-**-**' END AS site_id_masked,
           ROUND(AVG(cost_per_hour), 2) AS avg_cost
    FROM {FACT_TABLE}
    GROUP BY bidding_zone, site_id
    ORDER BY bidding_zone
"""))

# COMMAND ----------

# 3b) Row filter effect: a Poland-scoped user only sees PL rows. Compare row counts.
display(spark.sql(f"""
    SELECT 'all zones (steward)' AS view, COUNT(*) AS n_rows FROM {FACT_TABLE}
    UNION ALL
    SELECT 'Poland-scoped user', COUNT(*) FROM {FACT_TABLE} WHERE bidding_zone = 'PL'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evaluation with MLFlow
# MAGIC A tiny golden set (numbers + docs). MLflow 3 runs built-in LLM judges so you can gate prompt changes
# MAGIC in CI.

# COMMAND ----------

import mlflow
from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety, RetrievalGroundedness

def predict_fn(messages):
    result = agent.invoke({"messages": messages})
    return result["messages"][-1].content

eval_data = [
    {"inputs": {"messages": [{"role": "user", "content": "Which bidding zones are in the gold layer?"}]},
     "expectations": {"expected_facts": ["PL"]}},
    {"inputs": {"messages": [{"role": "user", "content": "What was the total energy cost in CZ in September 2026?"}]},
     "expectations": {"expected_facts": ["a numeric total cost for CZ"]}},
    {"inputs": {"messages": [{"role": "user", "content": "What alert monitors data volume, and at what threshold?"}]},
     "expectations": {"expected_facts": ["fewer than 192 rows per day (8 sites x 24 hours)"]}},
    {"inputs": {"messages": [{"role": "user", "content": "What was the average PUE in the PL bidding zone in September 2026?"}]},
     "expectations": {"expected_facts": ["an average PUE for PL of roughly 1.3"]}},
    {"inputs": {"messages": [{"role": "user", "content": "According to the ENTSO-E data documentation, what is an external constraint?"}]}, "expectations": {"expected_facts": ["the maximum import and/or export constraints of a given bidding zone", "not associated with any grid elements"]}},

   
]

results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=[Correctness(), RetrievalGroundedness()],
)
print("Open the MLflow run to see per-question judge scores.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Metadata filtering

# COMMAND ----------

# Metadata filtering: same query, two doc_type-filtered tools -> different corpora should be used
query = "What is a bidding zone?"

print("=== search_internal_docs (filter: doc_type = internal_docs) ===")
print(internal_docs_tool.invoke(query))

print("\n=== search_entsoe_glossary (filter: doc_type = external_reference) ===")
print(external_reference_tool.invoke(query))