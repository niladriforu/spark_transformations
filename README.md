# Spark Transformations — Local Dev Stack

A Docker-based environment with **Apache Spark**, **PySpark notebooks**, and **Unity Catalog** fully integrated. Use Spark SQL from Jupyter to browse catalogs, read managed tables, and create new Delta tables that appear in the Unity Catalog UI.

## What runs

| Service | URL | Purpose |
|---------|-----|---------|
| **Spark Master UI** | http://127.0.0.1:8080 | Cluster only — workers and a list of running apps (no Jobs/Stages) |
| **Spark Application UI** | http://127.0.0.1:4040 | Per-notebook driver UI — Jobs, Stages, Storage, Executors, SQL |
| **Jupyter (PySpark)** | http://127.0.0.1:8888 | Notebooks — primary way to run Spark + UC code |
| **Sample notebook** | http://127.0.0.1:8888/notebooks/spark_transformation_setup.ipynb | Pre-built UC integration walkthrough |
| **Unity Catalog UI** | http://127.0.0.1:3000 | Browse catalogs, schemas, and tables |
| **Unity Catalog API** | http://127.0.0.1:8081 | REST API (`Hello, Unity Catalog!` at root) |
| **MLflow** | http://127.0.0.1:5001 | Experiment tracking (optional) |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Internet access on first Spark session start (Maven downloads Delta + Unity Catalog jars)

## Project layout

```
spark_transformations/
├── README.md
├── important_links              # Quick URL reference
├── data/                        # Shared data volume (mounted as /data in containers)
└── setup/
    ├── docker-compose.yml       # All services
    ├── Dockerfile               # Jupyter + PySpark image
    └── app/
        ├── spark_uc.py          # Spark session factory, UC config, Jupyter display() helper
        └── spark_transformation_setup.ipynb
```

## Setup steps

### 1. Clone and enter the setup directory

```bash
git clone <your-repo-url>
cd spark_transformations/setup
```

### 2. Build the Jupyter image

The Jupyter container includes PySpark 3.5.3, Delta Lake 3.2.1, and the Unity Catalog Spark connector.

```bash
docker compose build jupyter
```

### 3. Start all services

```bash
docker compose up -d
```

Verify everything is running:

```bash
docker compose ps
```

You should see `spark-master`, `spark-worker`, `unity-catalog`, `unity-catalog-ui`, `jupyter`, and `mlflow` in the **Up** state.

### 4. Open the UIs

| Step | Action |
|------|--------|
| Spark cluster UI | Open http://127.0.0.1:8080 — confirm 1 worker is registered |
| Spark application UI | After the first notebook cell, open http://127.0.0.1:4040 for Jobs/Stages/SQL |
| Unity Catalog UI | Open http://127.0.0.1:3000 — browse `unity` → `default` → Tables |
| Jupyter | Open http://127.0.0.1:8888 — no token/password required |

### 5. Run the sample notebook

1. Open http://127.0.0.1:8888/notebooks/spark_transformation_setup.ipynb
2. Run the cells in order
3. **First cell only:** the first Spark session start downloads jars from Maven (~1–2 minutes). Later runs are much faster.

### 6. Confirm integration

After running the notebook cells you should see:

- `SHOW CATALOGS` lists `unity` (and `spark_catalog`)
- `SHOW TABLES IN unity.default` lists demo tables like `marksheet`
- `SELECT * FROM unity.default.marksheet LIMIT 5` returns rows
- A new table appears under **unity → demo → Tables** in the UC UI after the create step

---

## How Spark + Unity Catalog are wired

Spark does **not** talk to Unity Catalog out of the box. This project adds the required connector and config via `setup/app/spark_uc.py`:

| Config | Value |
|--------|-------|
| Spark | 3.5.3 (cluster + PySpark driver) |
| Delta Lake | 3.2.1 |
| UC Spark connector | `unitycatalog-spark_2.12:0.2.1` |
| UC server URI (inside Docker) | `http://unity-catalog:8081` |
| Default catalog | `unity` |

Create a session from any notebook or script:

```python
import importlib
import spark_uc

importlib.reload(spark_uc)  # pick up spark_uc.py edits without a kernel restart
from spark_uc import create_spark_session, print_session_info

spark = create_spark_session(app_name="MyApp")
print_session_info(spark)
```

`create_spark_session()` also patches notebook `display()` so Spark DataFrames render as HTML tables (see [Viewing DataFrames](#viewing-spark-dataframes-in-jupyter) below).

Environment variables (set in `docker-compose.yml` for the Jupyter service):

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER` | `spark://spark-master:7077` | Spark cluster URL |
| `UC_URI` | `http://unity-catalog:8081` | Unity Catalog API endpoint |
| `UC_CATALOG` | `unity` | Default SQL catalog |

> **Note:** Use `http://unity-catalog:8081` from inside Docker. Use `http://127.0.0.1:8081` only when running PySpark on your host machine.

---

## Viewing Spark DataFrames in Jupyter

If you are used to **Databricks notebooks**, `display(df)` there renders an interactive HTML table. **Plain Jupyter does not do that out of the box.**

### What goes wrong

| What you run | What you get |
|--------------|--------------|
| `display(df.limit(3))` (no setup) | Schema string only, e.g. `DataFrame[borough: string, crash_date: string, ...]` — **not rows** |
| `from spark_uc import display` after an old kernel import | `ImportError: cannot import name 'display'` — stale module cache |

Jupyter ships with **IPython's** `display()`, which does not know how to render Spark DataFrames. It prints the DataFrame repr (column names and types), which looks like a schema dump.

### What this project provides

`setup/app/spark_uc.py` defines a Spark-aware `display()` that converts the DataFrame to a pandas table and renders it in the notebook. When you call `create_spark_session()`, it **patches** `display()` in the notebook namespace automatically.

After running the first notebook cell you should see:

```
Notebook display() patched for Spark DataFrames.
```

Then preview rows like this:

```python
df = spark.table("demo.collisions_final")
display(df.limit(3))
```

Optional explicit import (works after the first cell has loaded/reloaded `spark_uc`):

```python
from spark_uc import display

display(df.limit(3))
```

Console output (no HTML table) still works with Spark's built-in method:

```python
df.limit(3).show()
df.show(3, truncate=False)
```

### Picking up `spark_uc.py` changes

The sample notebook's first cell uses `importlib.reload(spark_uc)` so edits to `spark_uc.py` on the host are picked up **without a full kernel restart** (the file is bind-mounted at `/app/spark_uc.py` in the Jupyter container).

If `display` still misbehaves after editing `spark_uc.py`:

1. **Re-run the first notebook cell** (reload + `create_spark_session()`), or
2. **Kernel → Restart** and run all cells from the top, or
3. Reload manually in any cell:

```python
import importlib
import spark_uc

importlib.reload(spark_uc)
from spark_uc import display

display(df.limit(3))
```

### Use the Docker Jupyter kernel

Run notebooks against the Docker Jupyter server at **http://127.0.0.1:8888**, not your local Mac Python. Local Python will not have PySpark or the UC connector wired up.

In Cursor: **Cmd+Shift+P** → `Jupyter: Select Jupyter Server` → `http://127.0.0.1:8888`

---

## Sample table creation (end-to-end)

Run this in a Jupyter cell after `create_spark_session()`:

```python
from spark_uc import create_spark_session

spark = create_spark_session(app_name="TableCreationDemo")

# 1. Create a schema
spark.sql("CREATE SCHEMA IF NOT EXISTS demo")

# 2. Create an external Delta table (metadata in UC, data on shared /data volume)
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.collisions (
  crash_date STRING,
  borough STRING,
  zip_code STRING,
  latitude DOUBLE,
  longitude DOUBLE
)
USING delta
LOCATION 'file:///data/tables/collisions'
""")

# 3. Verify in Spark
spark.sql("SHOW TABLES IN demo").show(truncate=False)
```

Expected output from `SHOW TABLES`:

```
+---------+----------+-----------+
|namespace|tableName |isTemporary|
+---------+----------+-----------+
|demo     |collisions|false      |
+---------+----------+-----------+
```

Then refresh the Unity Catalog UI: **Catalogs → unity → demo → Tables** — `collisions` should appear.

### Insert sample data (optional)

```python
spark.sql("""
INSERT INTO demo.collisions VALUES
  ('2024-01-01', 'MANHATTAN', '10001', 40.75, -73.99),
  ('2024-01-02', 'BROOKLYN',  '11201', 40.69, -73.99)
""")

spark.sql("SELECT * FROM demo.collisions").show()
```

### Read an existing managed demo table

```python
spark.sql("SELECT * FROM unity.default.marksheet LIMIT 5").show()
```

---

## Creating catalogs and schemas in the UI

The Unity Catalog UI supports **creating catalogs and schemas**, but **not tables**. Tables must be created via:

- **PySpark / Spark SQL** (recommended — see above), or
- **Unity Catalog REST API** at http://127.0.0.1:8081

Catalogs and schemas you create in the UI are persisted in the `uc_metastore` Docker volume across restarts.

To use a custom catalog from Spark (e.g. one you created in the UI):

```python
spark = create_spark_session(
    default_catalog="my_catalog",
    extra_catalogs=(),
)
```

---

## Spark UI: two different pages

Spark exposes **two** web UIs. They are easy to confuse — especially if you are used to Databricks.

### 1. Spark Master UI — http://127.0.0.1:8080

This is the **cluster manager** page. It shows:

- Registered workers
- A list of running applications (name + link)

It does **not** show Jobs, Stages, Storage, Executors, or SQL plans. If you are on port **8080** and looking for those tabs, you are on the wrong page.

### 2. Spark Application UI — http://127.0.0.1:4040

This is the **driver** UI for your notebook session. PySpark runs the driver inside the **Jupyter** container, so the detailed UI lives there — not on the master.

After `create_spark_session()`, the first notebook cell prints the URL:

```
Spark application UI: http://127.0.0.1:4040
```

Open that page to see:

| Tab | What it shows |
|-----|----------------|
| **Jobs** | Completed and running jobs |
| **Stages** | Stage-level task progress |
| **Storage** | Cached/persisted RDDs and DataFrames |
| **Environment** | Spark config and classpath |
| **Executors** | Per-executor metrics |
| **SQL** | SQL/DataFrame query plans and duration |

There is **no separate "DataFrame" tab** in open-source Spark (Databricks adds extra UI). DataFrame operations appear under **SQL** and **Jobs** once they execute.

### When tabs look empty

The UI only populates **after Spark actually runs work**. Defining `df = spark.read...` is lazy — nothing appears until an action such as:

```python
df.limit(3).show()
df.count()
df.write.save(...)
```

Run a cell that triggers computation, then refresh http://127.0.0.1:4040.

### The UI only exists while your notebook kernel is running

The Spark driver lives inside the Jupyter container. If you open http://127.0.0.1:4040 **before** running `create_spark_session()`, or after the kernel was stopped/restarted, the browser gets **connection reset** / blank page — Docker forwards the port, but nothing is listening yet.

**Fix:** run the first notebook cell, confirm you see `Spark UI status: reachable`, then open the printed URL.

### Port 4040 vs 4041

If another Spark session already holds port 4040 (second notebook kernel, zombie driver), Spark silently moves the UI to **4041**, **4042**, etc. Opening 4040 will fail even though your session is healthy.

The first notebook cell prints the **actual** URL (e.g. `http://127.0.0.1:4041`). Always use that URL, not 4040 by habit.

`docker-compose.yml` maps **4040–4045** so fallback ports work from your browser. After changing compose ports:

```bash
docker compose up -d jupyter
```

Then **restart the notebook kernel** and re-run the first cell.

### Link from Master UI does not open

On http://127.0.0.1:8080, clicking a running application may link to an internal Docker hostname (e.g. `jupyter:4040`) that your browser cannot resolve. Use **http://127.0.0.1:4040** directly instead.

---

## Stopping and restarting

```bash
# Stop all services
docker compose down

# Start again (data and UC metastore persist in Docker volumes)
docker compose up -d

# Rebuild Jupyter after Dockerfile changes
docker compose build jupyter && docker compose up -d jupyter
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| First Spark cell is slow | Normal — Maven is downloading Delta and UC jars. Wait 1–2 minutes. |
| `display(df)` shows schema only (`DataFrame[col: type, ...]`) | Re-run the first notebook cell so `create_spark_session()` patches `display()`. Do not rely on IPython's built-in `display`. See [Viewing DataFrames](#viewing-spark-dataframes-in-jupyter). |
| `ImportError: cannot import name 'display' from 'spark_uc'` | Kernel has a stale cached copy of `spark_uc` from before `display` was added. Re-run the first cell (`importlib.reload`) or restart the kernel. |
| `pyspark is not installed` / wrong kernel | Point Jupyter at the Docker server: http://127.0.0.1:8888 (see [Viewing DataFrames](#viewing-spark-dataframes-in-jupyter)). |
| `Catalog not found` | Create the catalog in the UC UI first, or use the built-in `unity` catalog. |
| `DELTA_TABLE_NOT_FOUND` on managed tables | Ensure all services were started with `docker compose up -d` so the shared `uc_managed_data` volume is mounted. |
| Unity Catalog UI shows no new table | Refresh the page. Confirm you ran `CREATE TABLE` against the `unity` catalog (or your configured default). |
| Port 5000 in use (macOS) | MLflow is mapped to **5001** intentionally because macOS often uses 5000 for AirPlay. |
| No Jobs/Stages/SQL tabs in Spark UI | You are likely on the **Master UI** (8080). Use the **Application UI** at http://127.0.0.1:4040 after `create_spark_session()`. See [Spark UI](#spark-ui-two-different-pages). |
| Spark UI at 4040 does not load / connection reset | (1) Re-run the first notebook cell — UI exists only while the kernel + Spark session are alive. (2) Use the **exact URL** printed by `print_session_info()` — it may be 4041+ if 4040 is busy. (3) Run `docker compose up -d jupyter` if ports 4040–4045 were recently added. |
| Jobs/Stages tabs are empty | Normal until you run an **action** (`.show()`, `.count()`, `.write()`, etc.). Lazy transforms alone do not create jobs. |

---

## Version requirements

Per [Unity Catalog Spark integration docs](https://docs.unitycatalog.io/integrations/unity-catalog-spark/):

- Apache Spark **≥ 3.5.3**
- Delta Lake **≥ 3.2.1**
- Unity Catalog **≥ 0.2**

This project pins those versions in `setup/Dockerfile` and `setup/docker-compose.yml`.
