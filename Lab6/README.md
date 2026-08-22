# Lab 6 – Gold Layer, Business Analytics & Governance

Goal: present the data from a business perspective. This lab builds the gold layer on top of the
Lab 5 declarative pipeline, exposes it through an AI/BI dashboard and a Genie space, secures it with
governance (grants, row-level and column-level security), and monitors it with a data-volume alert.

## What was done

### 1. Gold layer inside the declarative pipeline (LDP rework)

Per the instructor's guidance, the gold layer was built **inside the Lakeflow declarative pipeline**
(`Lab5/ldp_pipeline.py`), not as a separate imperative notebook. Gold tables are published to a
dedicated **`gold` schema** using fully-qualified dataset names; `gold_schema` was added as a variable
and pipeline `configuration` key in `databricks.yml`, and the schema is created in `Lab5/00_setup`.

Datasets added (all `@dp.materialized_view`):

- **`silver_datacenter`** (default/silver schema) – `SELECT DISTINCT` of site attributes from
  `sensor_silver` (source for the datacenter dimension).
- **`consumption_hourly`** (gold, **fact**) – grain: one row per site per hour. Built from
  `sensor_silver LEFT JOIN prices_silver` on truncated hour + `bidding_zone`. Measures:
  `avg_consumption_kwh`, `avg_power_kw`, `avg_pue`, and
  `cost_per_hour = ROUND(AVG(consumption_kwh * price / 1000), 2)`.
- **`dim_datacenter`** (gold, dimension) – from `silver_datacenter`.
- **`dim_date`** (gold, dimension) – distinct dates from the fact, with `month`, `day`, `week`, `year`,
  `day_of_week`, `day_of_week_name`, `month_name`, `quarter`, `is_weekend`.

Star schema: **1 fact (`consumption_hourly`) + 2 dimensions (`dim_datacenter`, `dim_date`)**.

Sensor `consumption_kwh` was also made realistic and consistent with `avg_power_kw` (derived as
power × interval, with a small random variation) in both the generator and the Event Hub producer, so
cost figures are plausible. `Lab6/bronze_silver_removal.ipynb` was used once to drop the old imperative
gold tables so the pipeline could manage its own.

### 2. AI/BI dashboard – "Datacenter Energy Cost Dashboard"

Single dataset `consumption_hourly`, joined to `dim_date` (so time dimensions like day-of-week are
available to the charts and filters).

- **KPI counters:** Total Cost, Average PUE, Date, Day of the Week.
- **Charts:** average hourly consumption per site (line); average hourly PUE per site (line); total
  cost per bidding zone (bar); average cost over date (line trend).
- **Global filters (single-select):** Bidding zone, Date, Day of the Week.

![Datacenter Energy Cost Dashboard](./screenshots/dashboard.png)

### 3. Governance – grants + RLS + CLS  (`Lab6/07_RLS_CLS`)

- **Group:** workspace group **`Poland`** (the user is a member; membership checked with `is_member`).
- **Grant permissions to objects:**
  ```sql
  GRANT SELECT ON TABLE consumption_hourly TO `account users`;
  ```
  Note: the `GRANT` targets the account-level group **`account users`**, because Unity Catalog `GRANT`
  requires an **account-level** principal – a **workspace** group (`Poland`) cannot be a grant target.
  (`Poland` is still used for RLS/CLS, where `is_member` does work with workspace groups.)
- **Row-Level Security (RLS):** `regional_filter(bidding_zone)` returns BOOLEAN –
  a user who is a member of **both `Poland` and `admins`** sees only `PL` rows; everyone else sees none.
  Applied with:
  ```sql
  ALTER MATERIALIZED VIEW consumption_hourly SET ROW FILTER regional_filter ON (bidding_zone);
  ```
  (`ALTER MATERIALIZED VIEW`, not `ALTER TABLE`, because the gold object is a materialized view.)
- **Column-Level Security (CLS):** `site_id_mask(site_id)` masks non-PL `site_id` to `'**-**-**'` for
  `Poland` members (PL sites shown in full; other users see everything). Applied with:
  ```sql
  ALTER MATERIALIZED VIEW consumption_hourly ALTER COLUMN site_id SET MASK site_id_mask;
  ```
- Governance is applied **outside** the pipeline and was verified to survive a data full-refresh.
- **Note:** the notebook also contains `DROP ROW FILTER` / `DROP MASK` cells (used to test the
  before/after effect). To leave RLS/CLS **enforced** in the final state, do **not** run those drop
  cells – or re-apply the `SET ROW FILTER` / `SET MASK` at the end.

Column mask visible on the dashboard – the `site_id` legend shows the PL site (`DC-PL-01`) in full and
every other site collapsed to the masked value `**-**-**`:

![Column mask applied – site_id masked on the dashboard](./screenshots/cls_mask_dashboard.png)

### 4. Genie space

A Genie space over the gold tables answers natural-language questions, e.g.
*"What is the monthly total consumption in kWh for each bidding zone?"* → per-zone chart with a written
summary. (Note: the sample data starts ~24 Jul, so July is a partial month – monthly totals reflect day
coverage, not a real month-over-month jump.)

![Genie space – natural-language Q&A over the gold layer](./screenshots/genie.png)

### 5. Alert – data-volume drop → email  (`Lab6/Volume Drop Alert`)

Watches how many rows exist for a day; normal volume = 8 sites × 24 hours = **192** rows/day.
```sql
SELECT COUNT(*) AS rows_today
FROM workspace.gold.consumption_hourly
WHERE date = current_date()
```
- Condition: `rows_today < 192`. Schedule: daily (18:05, Europe/Warsaw). Notification: email.
- **Simulated drop:** with historical sample data, `current_date()` returns 0 rows → `0 < 192` → the
  alert **triggers** and the email is delivered (screenshot).
- **Healthy contrast:** `WHERE date = '2026-08-21'` → `192` → `192 < 192` is false → status **OK**, no
  trigger (screenshot). *(The saved alert currently uses this fixed date for the OK demo; to arm it as a
  live monitor, switch the `WHERE` back to `current_date()`.)*

Alert condition triggered (`current_date()` → 0 rows < 192):

![Volume Drop Alert – triggered condition](./screenshots/alert_triggered.png)

Email notification received:

![Volume Drop Alert – email notification](./screenshots/alert_email.png)

Healthy contrast (`date = '2026-08-21'` → 192 rows, status OK, no trigger):

![Volume Drop Alert – OK / not triggered](./screenshots/alert_ok.png)

## Run order

```
Lab5/00_setup  (schemas incl. gold + landing Volume)
   -> generate_bronze_sample (dev)  /  real fetch + Event Hub (prod)
   -> Lab5/ldp_pipeline  (bronze -> silver -> gold, via the bundle)
   -> Lab6/07_RLS_CLS  (grant + row filter + column mask)   [skip the DROP cells to keep them enforced]
   -> dashboard / Genie / alert  (consumption, outside the pipeline)
```

## Requirements coverage

- [x] Gold **star schema** – 1 fact + 2 dimensions, with aggregations.
- [x] **AI/BI dashboard** with filters and multiple visualizations.
- [x] **Genie space** (optional) for natural-language Q&A.
- [x] **Alert** fires on a simulated data-volume drop and sends an email.
- [x] **Governance** – grant to objects (`account users`) + RLS + CLS.

## Files

- `Lab5/ldp_pipeline.py` – includes the gold layer.
- `Lab5/00_setup.ipynb` – creates the gold schema.
- `databricks.yml` – `gold_schema` variable + pipeline configuration.
- `Lab6/07_RLS_CLS.ipynb` – grant, row filter, column mask.
- `Lab6/bronze_silver_removal.ipynb` – one-off cleanup of old imperative gold tables.
- `Lab6/Datacenter Energy Cost Dashboard.lvdash.json` – the dashboard.
- `Lab6/Volume Drop Alert.dbalert.json` – the alert.
