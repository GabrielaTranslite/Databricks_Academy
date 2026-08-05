# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FreshBite – Data Layout Optimization - handout
# MAGIC **Vacuum  ·  Liquid Clustering  ·  Z-Order vs Liquid Clustering  ·  Decision guidelines**
# MAGIC
# MAGIC This notebook doubles as the handout for my part of the talk. The numbered theory sections are the notes. The demo near the end proves the Liquid Clustering and Z-Order data-skipping win on real data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The FreshBite scenario
# MAGIC FreshBite is a food-delivery startup running on one giant `orders` Delta table. About 2 TB and growing, queried all day (orders in Warsaw yesterday, this restaurant last month, refunds in Q2). It bills for 9 TB of storage, and a simple "yesterday in Warsaw" query scans far more than it should. Maya, the data engineer, owns the fix.
# MAGIC
# MAGIC Two symptoms, and where each is solved:
# MAGIC - Slow queries  →  fix the layout (Liquid Clustering or Z-Order)
# MAGIC - Rising storage bill  →  clean up dead files (VACUUM)
# MAGIC
# MAGIC What is inside:
# MAGIC 1. VACUUM and the 7-day rule
# MAGIC 2. Liquid Clustering, how it works and how to use it
# MAGIC 3. Z-Order vs Liquid Clustering
# MAGIC 4. Decision guidelines
# MAGIC 5. The demo, build one table three ways and measure data skipping
# MAGIC 6. Results, what Maya achieved

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. VACUUM and the 7-day rule
# MAGIC Delta never overwrites in place. Updates, deletes and merges write new files and mark old ones removed. Old files stay so you can time-travel.
# MAGIC
# MAGIC `VACUUM` permanently deletes files that are no longer referenced by the table AND older than the retention window.
# MAGIC
# MAGIC Key point. VACUUM reclaims storage, it does not make queries faster. OPTIMIZE and clustering make queries fast.
# MAGIC
# MAGIC Example, for reference:
# MAGIC ```sql
# MAGIC -- see what would be deleted, without deleting anything
# MAGIC VACUUM orders DRY RUN
# MAGIC -- reclaim storage for unreferenced files older than the retention threshold
# MAGIC VACUUM orders
# MAGIC ```
# MAGIC
# MAGIC Retention and the 7-day rule:
# MAGIC - Long retention gives more time travel and audit, but costs storage.
# MAGIC - Short retention is cheap, but your time-travel window shrinks to match.
# MAGIC - Databricks strongly recommends a retention interval of at least 7 days. This is both the default and the enforced floor.
# MAGIC - Why 7 days. A long-running job can write files it has not committed yet. A shorter window could VACUUM those away (data loss), and your travel window shrinks with it.
# MAGIC - Set retention per table, by your audit need. FreshBite uses 30 days on `orders` and 7 on throwaway staging tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Liquid Clustering
# MAGIC Partitioning splits a table into folders by a column. It is rigid, chosen once, and high-cardinality or skewed keys create thousands of tiny files and skew.
# MAGIC
# MAGIC Liquid Clustering is the modern alternative. You declare clustering keys and Databricks organises data into balanced clusters automatically and incrementally. No folders.
# MAGIC
# MAGIC How it works:
# MAGIC - Merges small files and splits large ones into the 16 MB – 1 GB target size, so reads stay fast.
# MAGIC - Incremental. OPTIMIZE rewrites only new or affected files, not the whole table, so cost stays low.
# MAGIC - Change the clustering keys later without rewriting existing data.
# MAGIC - Great for high-cardinality filters, heavy skew, fast-growing tables, and tables where a partition key returns too many or too few partitions.
# MAGIC
# MAGIC How you use it:
# MAGIC ```sql
# MAGIC CREATE TABLE orders (...) CLUSTER BY (city, order_date)
# MAGIC ALTER TABLE orders CLUSTER BY (restaurant_id)   -- change keys, no rewrite
# MAGIC OPTIMIZE orders                                  -- cluster new or affected data
# MAGIC OPTIMIZE orders FULL                             -- recluster everything once
# MAGIC ```
# MAGIC
# MAGIC Converting an existing table. Enable LC and pick keys from your most-used query filters. Existing partition columns are a good starting point. Then run OPTIMIZE.
# MAGIC
# MAGIC Limits:
# MAGIC - Up to 4 clustering keys. For tables under 10 TB, more keys can hurt single-column filters.
# MAGIC - Clustering keys need statistics, collected on the first 32 columns of a Delta table by default.
# MAGIC - Not compatible with partitioning or ZORDER. LC replaces both.
# MAGIC - Since 2025, Databricks recommends LC for all new tables. Automatic LC (`CLUSTER BY AUTO`) can pick keys for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Z-Order vs Liquid Clustering
# MAGIC | Aspect | Z-Ordering | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | What it is | Sorts and co-locates data within partitions on chosen columns | Adaptive clustering of the whole table. Replaces partitioning |
# MAGIC | When it runs | Manual OPTIMIZE ... ZORDER BY, re-run after big writes | Incremental via OPTIMIZE. Can be fully automatic |
# MAGIC | Changing keys | Rewrites data, tied to your partition scheme | Change keys with ALTER, no rewrite |
# MAGIC | Small files and skew | Still needs partitions, so skew and small-file risk | Managed automatically |
# MAGIC | Databricks 2025 | Still supported, no longer the default advice | Recommended for all new tables |
# MAGIC
# MAGIC Z-Order optimises a layout you committed to. Liquid Clustering removes the commitment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Decision guidelines
# MAGIC - New table  →  Liquid Clustering, the default in 2025 and later
# MAGIC - Partitioned table you can rebuild  →  migrate it to Liquid Clustering
# MAGIC - Partitioned table you cannot touch  →  OPTIMIZE + ZORDER BY on the filter columns
# MAGIC - Table under about 1 TB  →  do not partition, use LC
# MAGIC - Storage bill or time travel  →  set per-table VACUUM retention to your audit need
# MAGIC - Do not want to manage any of it  →  Predictive Optimization runs OPTIMIZE, VACUUM and clustering for you

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The demo, proving data skipping
# MAGIC We build one FreshBite `orders` table three ways (baseline, Z-Order, Liquid Clustering) and run the same selective query on each.
# MAGIC
# MAGIC Expect the baseline to scan almost all files. Z-Order and Liquid read just a handful. That gap is the whole point.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Config

# COMMAND ----------

CATALOG = "workspace"     # Free Edition default catalog; change if yours differs
SCHEMA  = "freshbite"
N_ROWS  = 50_000_000      # ~1-2 GB. Lower to 20M if you hit quota.
N_FILES = 200             # how many files to scatter the baseline across

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print("Using", CATALOG, SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Build the baseline FreshBite `orders` table
# MAGIC
# MAGIC Every column is random and independent, and we `repartition` so each file spans the **full**
# MAGIC `restaurant_id` range. That means a filter on one restaurant can't skip any file – the worst case,
# MAGIC and exactly what an un-optimized table looks like.

# COMMAND ----------

from pyspark.sql import functions as F

cities = ["Warsaw", "Krakow", "Wroclaw", "Poznan", "Gdansk", "Lodz", "Katowice", "Lublin"]
city_arr = F.array(*[F.lit(c) for c in cities])

base = (spark.range(0, N_ROWS)
        .withColumnRenamed("id", "order_id")
        .withColumn("customer_id",   (F.rand(seed=1) * 2_000_000).cast("int"))
        .withColumn("restaurant_id", (F.rand(seed=2) * 50_000).cast("int"))
        .withColumn("city",          F.element_at(city_arr, (F.floor(F.rand(seed=3) * len(cities)) + 1).cast("int")))
        .withColumn("order_date",    F.expr("date_add('2025-01-01', cast(rand() * 365 as int))"))
        .withColumn("amount",        F.round(F.rand(seed=4) * 120 + 5, 2))
        .repartition(N_FILES))       # scatter rows so no natural clustering exists

(base.write.format("delta").mode("overwrite").saveAsTable("orders_baseline"))


# Predictive Optimization is on by default for managed tables and would auto-cluster it, so we have to disable it:
spark.sql("ALTER TABLE orders_baseline DISABLE PREDICTIVE OPTIMIZATION")
print("baseline built")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL orders_baseline

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Build the **Z-Ordered** copy
# MAGIC Same data, then `OPTIMIZE ... ZORDER BY` on the columns FreshBite filters on.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_zorder AS SELECT * FROM orders_baseline;
# MAGIC ALTER TABLE orders_zorder DISABLE PREDICTIVE OPTIMIZATION;
# MAGIC OPTIMIZE orders_zorder ZORDER BY (restaurant_id, order_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_zorder

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Build the **Liquid Clustering** copy
# MAGIC Declare `CLUSTER BY` at create time, then `OPTIMIZE FULL` to cluster the loaded data.
# MAGIC (Liquid Clustering is *incompatible* with partitioning and ZORDER – it replaces both.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_liquid
# MAGIC   CLUSTER BY (restaurant_id, order_date)
# MAGIC   AS SELECT * FROM orders_baseline;
# MAGIC OPTIMIZE orders_liquid FULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_liquid

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Layout check – how many files, how big
# MAGIC After OPTIMIZE, both optimized tables pack data into fewer, well-organized files.

# COMMAND ----------

import pandas as pd
rows = []
for t in ["orders_baseline", "orders_zorder", "orders_liquid"]:
    d = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").first()
    rows.append((t, d["numFiles"], round(d["sizeInBytes"] / 1e6, 1)))
display(pd.DataFrame(rows, columns=["table", "numFiles", "sizeMB"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM on the FreshBite table (Part 1 in action)
# MAGIC `DESCRIBE HISTORY` shows the versions VACUUM can clean up behind. `DRY RUN` previews what would be removed and deletes nothing. On a freshly built table there may be little to reclaim, that is expected. To actually reclaim storage you would run `VACUUM orders_baseline`, which respects the 7-day retention floor by default.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- History = the versions and old files VACUUM can clean behind
# MAGIC DESCRIBE HISTORY orders_baseline;
# MAGIC -- Preview what VACUUM would delete. DRY RUN removes nothing.
# MAGIC VACUUM orders_baseline DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. The measurement – same selective query on each table
# MAGIC
# MAGIC FreshBite question: *"orders for restaurant 27384 in June 2025"*. This is selective + high-cardinality
# MAGIC on `restaurant_id`, so it's the ideal case for data skipping.
# MAGIC
# MAGIC Run each of the three SQL cells below, and after each one **See performance > View all in query history**
# MAGIC On the scan node read:
# MAGIC - **files read** / **bytes read**
# MAGIC - **bytes pruned**
# MAGIC
# MAGIC The baseline reads ~all files. the Z-Order and Liquid tables read a handful. That gap is the point.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BASELINE (expect: reads ~all files, ~0 pruned)
# MAGIC SELECT count(*) AS orders, round(avg(amount), 2) AS avg_ticket
# MAGIC FROM orders_baseline
# MAGIC WHERE restaurant_id = 27384
# MAGIC   AND order_date BETWEEN '2025-06-01' AND '2025-06-30';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER (expect: reads far fewer files, most bytes pruned)
# MAGIC SELECT count(*) AS orders, round(avg(amount), 2) AS avg_ticket
# MAGIC FROM orders_zorder
# MAGIC WHERE restaurant_id = 27384
# MAGIC   AND order_date BETWEEN '2025-06-01' AND '2025-06-30';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LIQUID CLUSTERING (expect: similar strong pruning to Z-Order)
# MAGIC SELECT count(*) AS orders, round(avg(amount), 2) AS avg_ticket
# MAGIC FROM orders_liquid
# MAGIC WHERE restaurant_id = 27384
# MAGIC   AND order_date BETWEEN '2025-06-01' AND '2025-06-30';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. The extra Liquid-Clustering advantage – change keys with **no rewrite**
# MAGIC Query patterns shifted and FreshBite now filters by `customer_id`? With Liquid Clustering you just
# MAGIC re-declare the keys. With Z-Order there's no such command – you'd change the OPTIMIZE job (and, if it
# MAGIC were partitioned, a partition change would mean a full table rewrite).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_liquid CLUSTER BY (customer_id);   -- instant metadata change
# MAGIC -- new + re-optimized data now clusters on customer_id:
# MAGIC OPTIMIZE orders_liquid;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. What the demo shows
# MAGIC
# MAGIC | | Baseline | Z-Ordering | Liquid Clustering |
# MAGIC |---|---|---|---|
# MAGIC | Files read for the query | ~all | few | few |
# MAGIC | Bytes pruned | ~0% | high | high |
# MAGIC | How you maintain it | – | manual `OPTIMIZE ... ZORDER` | `OPTIMIZE` (can be automatic) |
# MAGIC | Change the keys later | – | tied to OPTIMIZE job / partition scheme | `ALTER TABLE ... CLUSTER BY`, no rewrite |
# MAGIC | Databricks 2025 guidance | – | legacy, still supported | recommended for new tables |
# MAGIC
# MAGIC Z-Ordering and Liquid Clustering deliver *similar* data-skipping wins on this query –
# MAGIC the difference is in flexibility and maintenance, not raw pruning. Both crush the un-optimized baseline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. (Optional) Clean up to free Free-Edition storage quota

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS orders_baseline;
# MAGIC -- DROP TABLE IF EXISTS orders_zorder;
# MAGIC -- DROP TABLE IF EXISTS orders_liquid;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Results, what Maya achieved
# MAGIC Back in the FreshBite story (illustrative numbers, your demo run will vary):
# MAGIC - Storage bill  9 TB  →  about 2 TB.  VACUUM with 30-day retention on `orders`.
# MAGIC - "Yesterday in Warsaw"  almost all files  →  a handful.  Data skipping from Liquid Clustering.
# MAGIC - Refund audit  still 30 days.  Time travel preserved where it matters.
# MAGIC - Upkeep  hands-off.  Scheduled OPTIMIZE and Predictive Optimization.
# MAGIC
# MAGIC **Same table, three fixes.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix, quick facts for Q&A
# MAGIC - Default VACUUM retention is 7 days (`delta.deletedFileRetentionDuration`). Log retention is about 30 days (`delta.logRetentionDuration`), but time travel needs the data files, so VACUUM is the real limit.
# MAGIC - `VACUUM ... DRY RUN` lists files that would be deleted without deleting them.
# MAGIC - Liquid Clustering merges small files and splits large ones into the 16 MB – 1 GB range. OPTIMIZE clusters incrementally, rewriting only new or affected files.
# MAGIC - LC keys. Up to 4 columns, changed with `ALTER TABLE ... CLUSTER BY`, keys need statistics (first 32 columns by default), incompatible with partitioning and ZORDER.
# MAGIC - Converting a table. Choose keys from your most-used query filters. Existing partition columns are a good start. `OPTIMIZE FULL` reclusters once.
# MAGIC - Predictive Optimization runs OPTIMIZE, VACUUM and ANALYZE on Unity Catalog managed tables. Default-on for accounts created since 11 Nov 2024, existing accounts rolled out around Aug 2026.
# MAGIC - Automatic Liquid Clustering (`CLUSTER BY AUTO`) picks and adapts keys from query history. GA on Databricks Runtime 15.4 LTS and above, Unity Catalog managed tables only.
# MAGIC
# MAGIC Sources. Databricks docs (VACUUM, liquid clustering, CLUSTER BY AUTO, predictive optimization, partitioning) and Databricks blog (Debunking 8 data layout myths, Announcing Automatic Liquid Clustering). Verified 26 Jul 2026.
