# Metadata_framwork

## Control Table Definition

This repository includes production-ready SQL and PySpark definitions for a lightweight Databricks Delta control table (`config.control_table`) that stores only execution metadata.

- SQL: `sql/create_control_table.sql`
- PySpark: `pyspark/create_control_table.ipynb`
- Config loader utility: `pyspark/config_loader.ipynb`

## ConfigLoader (PySpark)

`ConfigLoader` provides an enterprise-ready, modular utility for loading active source configuration from `config.control_table`.

### Behavior

- Accepts `source_name` as input
- Filters using:
  - `source_name = <input>`
  - `active_flag = true`
- Raises `ConfigNotFoundError` when no active record exists
- Returns the selected row as a Python dictionary
- Logs loaded config for debugging/operational traceability

### Example

```python
from pyspark.sql import SparkSession

from pyspark.config_loader import ConfigLoader

spark = SparkSession.builder.getOrCreate()
loader = ConfigLoader(spark)
config = loader.load_config("carrier_a")
```

## Ingestion (PySpark)

`Ingestion` provides a simple modular ingestion layer for Databricks pipelines.

### Methods

- `read(config)`
  - Reads data from `config["source_path"]` based on `config["source_type"]`
  - Supported source types: `csv` (with header = true), `parquet`
  - Logs the number of records read
  - Returns a PySpark DataFrame
- `write(df, config)`
  - Writes DataFrame to `config["target_table"]`
  - Uses default Spark table format with append mode

### Example

```python
from pyspark.sql import SparkSession

from pyspark.ingestion import Ingestion

spark = SparkSession.builder.getOrCreate()
ingestion = Ingestion(spark)

config = {
    "source_type": "csv",
    "source_path": "/mnt/raw/carrier_a",
    "target_table": "bronze.carrier_a",
}

df = ingestion.read(config)
ingestion.write(df, config)
```

## Standardizer (PySpark)

`Standardizer` provides config-driven enterprise standardization for raw vendor data.

### Highlights

- Loads mapping JSON from `config["mapping_config_path"]`
- Applies column mapping, missing column handling, defaults, type casting
- Standardizes strings, date/timestamp values (UTC), phone/postal formats
- Normalizes booleans and currency fields
- Deduplicates records using configured primary keys
- Applies a basic validation rule for `sales_amount`
- Optionally adds `record_year` and `record_month`
- Includes optional standard Spark `read(...)`/`write(...)` helpers with no Delta-only dependency

### Example

```python
from pyspark.standardizer import Standardizer

standardizer = Standardizer()
clean_df = standardizer.transform(df, {
    "mapping_config_path": "carrier_a_mapping.json",
    "source_timezone": "UTC",
    "add_derived_columns": True,
})
```
