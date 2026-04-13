"""Modular ingestion utilities for reading source data and writing to Delta tables."""

from __future__ import annotations

import logging
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

LOGGER = logging.getLogger(__name__)


class UnsupportedSourceTypeError(ValueError):
    """Raised when the source_type in configuration is not supported."""


class Ingestion:
    """Read source data and write processed DataFrames using Databricks conventions."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, config: Dict[str, Any]) -> DataFrame:
        """Read data based on source_type and source_path from config.

        Supported source_type values:
        - csv (header=true)
        - parquet
        """

        source_type = str(config.get("source_type", "")).strip().lower()
        source_path = str(config.get("source_path", "")).strip()

        if not source_path:
            raise ValueError("config['source_path'] must be a non-empty string.")

        if source_type == "csv":
            df = self.spark.read.option("header", "true").csv(source_path)
        elif source_type == "parquet":
            df = self.spark.read.parquet(source_path)
        else:
            raise UnsupportedSourceTypeError(
                f"Unsupported source_type '{source_type}'. Supported types are: csv, parquet."
            )

        record_count = df.count()
        LOGGER.info("Read %s records from %s source at path '%s'.", record_count, source_type, source_path)
        return df

    def write(self, df: DataFrame, config: Dict[str, Any]) -> None:
        """Write DataFrame to target Delta table in append mode."""

        target_table = str(config.get("target_table", "")).strip()
        if not target_table:
            raise ValueError("config['target_table'] must be a non-empty string.")

        df.write.format("delta").mode("append").saveAsTable(target_table)
        LOGGER.info("Appended DataFrame to Delta table '%s'.", target_table)
