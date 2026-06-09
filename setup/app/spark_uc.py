"""Spark session factory with Unity Catalog integration."""

import logging
import os

# Must be set before PySpark starts the JVM (quiets Ivy resolution spam).
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--conf spark.ui.showConsoleProgress=false "
    "--conf spark.eventLog.gcMetrics.enabled=false "
    "--driver-java-options=-Divy.message.logger.level=4 "
    "pyspark-shell",
)

logging.getLogger("py4j").setLevel(logging.ERROR)

try:
    from pyspark.sql import SparkSession
except ImportError as exc:
    raise ImportError(
        "pyspark is not installed in this Python environment. "
        "Run this notebook on the Docker Jupyter server at http://127.0.0.1:8888 "
        "(not local Mac Python). In Cursor: Cmd+Shift+P → "
        "'Jupyter: Select Jupyter Server' → http://127.0.0.1:8888"
    ) from exc

# Minimum versions per https://docs.unitycatalog.io/integrations/unity-catalog-spark/
UC_SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.1"

UC_URI = os.environ.get("UC_URI", "http://unity-catalog:8081")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
DEFAULT_CATALOG = os.environ.get("UC_CATALOG", "unity")

__all__ = ["create_spark_session", "print_session_info", "display", "spark_ui_url"]


def display(obj, n: int = 20, truncate: bool = True, **kwargs) -> None:
    """Render a Spark DataFrame as an HTML table in Jupyter (Databricks-style)."""
    from IPython.display import display as ipy_display
    from pyspark.sql import DataFrame as SparkDataFrame

    if isinstance(obj, SparkDataFrame):
        ipy_display(obj.limit(n).toPandas(), **kwargs)
    else:
        ipy_display(obj, **kwargs)


def _register_spark_display() -> None:
    """Patch notebook ``display()`` so bare calls work without an import."""
    try:
        from IPython import get_ipython
    except ImportError:
        return

    ipython = get_ipython()
    if ipython is None:
        return

    ipython.user_ns["display"] = display


def create_spark_session(
    app_name: str = "SparkUCApp",
    default_catalog: str = DEFAULT_CATALOG,
    extra_catalogs: tuple[str, ...] = (),
) -> SparkSession:
    """Create a Spark session wired to the local Unity Catalog server."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", UC_SPARK_PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.defaultCatalog", default_catalog)
        .config("spark.eventLog.gcMetrics.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
    )

    catalogs = tuple(dict.fromkeys((default_catalog, *extra_catalogs)))
    for catalog in catalogs:
        builder = (
            builder.config(f"spark.sql.catalog.{catalog}", "io.unitycatalog.spark.UCSingleCatalog")
            .config(f"spark.sql.catalog.{catalog}.uri", UC_URI)
            .config(f"spark.sql.catalog.{catalog}.token", "")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _register_spark_display()
    return spark


def spark_ui_url(spark: SparkSession, host: str = "127.0.0.1") -> str:
    """URL for the Spark *application* UI (Jobs, Stages, SQL) reachable from your browser."""
    port = spark.sparkContext.uiWebUrl.rsplit(":", 1)[-1]
    return f"http://{host}:{port}"


def _spark_ui_reachable(url: str) -> bool:
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return resp.status in (200, 302)
    except (urllib.error.URLError, TimeoutError, ValueError):
        return False


def print_session_info(spark: SparkSession) -> None:
    """Print key UC settings. Call after create_spark_session()."""
    ui_url = spark_ui_url(spark)
    ui_port = ui_url.rsplit(":", 1)[-1]

    print("Spark session ready.")
    print("Unity Catalog URI:", spark.conf.get(f"spark.sql.catalog.{DEFAULT_CATALOG}.uri"))
    print("Default catalog:", spark.conf.get("spark.sql.defaultCatalog"))
    print("Spark application UI:", ui_url)
    if ui_port != "4040":
        print(
            f"Note: port 4040 was busy — UI is on {ui_port}. "
            f"Open {ui_url} (not http://127.0.0.1:4040)."
        )
    if _spark_ui_reachable(ui_url):
        print("Spark UI status: reachable (keep this notebook kernel running).")
    else:
        print(
            "Spark UI status: not reachable yet. "
            "Re-run this cell or restart the kernel, then open the URL above."
        )
    print("Notebook display() patched for Spark DataFrames.")
