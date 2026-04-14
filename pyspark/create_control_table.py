"""Create config.control_table in Databricks using standard Spark format (no Delta-specific config)."""

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

# 1) Force-stop any existing Spark session to avoid carrying over stale configs.
try:
    SparkSession.builder.getOrCreate().stop()
except Exception:
    pass

# 2) Create a fresh standard Spark session without Delta-specific settings.
spark = (
    SparkSession.builder.appName("ControlTableSetup")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

# 3) Ensure schema exists.
spark.sql("CREATE SCHEMA IF NOT EXISTS config")

# 4) Define control table schema.
control_table_schema = StructType(
    [
        StructField("source_name", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("watermark_column", StringType(), True),
        StructField("last_processed_value", StringType(), True),
        StructField("mapping_config_path", StringType(), True),
        StructField("transformation_module", StringType(), True),
        StructField("active_flag", BooleanType(), True),
        StructField("config_version", StringType(), True),
    ]
)

# 5) Create an empty DataFrame and register table via standard Spark catalog write.
empty_df = spark.createDataFrame([], control_table_schema)
empty_df.write.mode("overwrite").saveAsTable("config.control_table")

print("Successfully created config.control_table using standard Spark format.")
