# Live-demo runbook - GenAI on Databricks (ENTSO-E)

Goal: a live demo that cannot embarrass you. The rule is simple - **anything slow or flaky is pre-run;
only the fast, visual parts happen on stage.**

## The evening before (PRE-RUN, ~15-20 min)

1. Import `GenAI_ENTSOE_demo.py` into the workspace and attach a cluster.
2. Set the config cell: `CATALOG = "workspace"`, `GOLD_SCHEMA = "gold"`, and check `DOCS_PATH` points at the
   folder that actually holds `Lab*/README.md` (Repos path or a Volume). Run the `list_bidding_zones()`
   smoke test to confirm the gold table and functions resolve.
3. Run section **1** (UC functions) and section **2** (docs table + Vector Search index). The index sync is
   the slow part - do it now, not on stage. Confirm the index shows **Online**.
4. Run section **5** (log + register + `agents.deploy`). Wait until the serving endpoint is **Ready**.
   Send it one test question so it is warm.
5. Open, in separate browser tabs ready to show: your **Genie space** over the gold layer, the **Vector
   Search index** page (status Online), the **serving endpoint** page, and the **MLflow eval run** from a
   trial run of section 6.
6. Take fallback **screenshots** of: agent answering the numeric question, agent answering the docs
   question, the eval judge scores. If the room's wifi dies, you still have the story.

## On stage (LIVE, ~4-5 min of the 12)

Run only these, in order:

1. **Genie space** (already open) - ask one natural-language question. "This is the no-code agent." (30s)
2. Notebook section **3** - show the agent definition (LLM + 2 SQL tools + 1 retriever), then run the two
   `agent.stream(...)` cells: one numeric, one documentation. This is the core moment. (2 min)
3. Notebook section **4** - run the `DESCRIBE EXTENDED` cell; point at the ROW FILTER and MASK lines.
   "The agent reads this exact table, so it inherits these. I wrote no access-control code." (1 min)
4. Section **6** - run the eval; open the MLflow run and show the judge scores. "This is my CI gate." (1 min)
5. Section **5** - **do not deploy on stage.** Switch to the already-live endpoint tab and say
   "deployed earlier, scale-to-zero, same object registered in Unity Catalog." (30s)

## If something fails live

- Agent cell errors -> switch to the fallback screenshot, keep talking, move on. Do not debug on stage.
- Endpoint slow -> that is why deployment was pre-run; use the warm endpoint tab.
- Wifi down -> the whole demo degrades gracefully to the screenshots; the slides still carry the argument.

## Free Edition limits to remember

One Vector Search endpoint / one search unit (Delta Sync only, no Direct Vector Access), Model Serving on
CPU with no provisioned throughput, Foundation Model APIs pay-per-token. All of this is enough for this
demo, but do not try to spin up a second index or a GPU endpoint.
