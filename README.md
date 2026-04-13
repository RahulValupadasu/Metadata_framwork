# Metadata_framwork

## Control Table Definition

This repository includes production-ready SQL and PySpark definitions for a lightweight Databricks Delta control table (`config.control_table`) that stores only execution metadata.

- SQL: `sql/create_control_table.sql`
- PySpark: `pyspark/create_control_table.py`
- Config loader utility: `pyspark/config_loader.py`

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
