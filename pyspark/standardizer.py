"""Config-driven enterprise standardization utilities for PySpark pipelines."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


class Standardizer:
    """Standardize raw vendor data into a clean, consistent DataFrame.

    Notes
    -----
    - The class is intentionally defensive and logs warnings for non-critical issues.
    - Mapping JSON should include: ``column_mappings``, ``data_types``,
      ``default_values``, and ``primary_keys``.
    """

    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """Apply config-driven standardization on ``df``.

        Parameters
        ----------
        df:
            Input raw PySpark DataFrame.
        config:
            Runtime config dictionary containing ``mapping_config_path`` and
            optional behavior toggles such as ``source_timezone`` and
            ``add_derived_columns``.
        """

        mapping = self._load_mapping_config(config)
        column_mappings = mapping.get("column_mappings", {})
        data_types = mapping.get("data_types", {})
        default_values = mapping.get("default_values", {})
        primary_keys = mapping.get("primary_keys", [])

        transformed_df = df
        transformed_df = self._rename_columns(transformed_df, column_mappings)
        transformed_df = self._add_missing_columns(
            transformed_df,
            expected_columns=self._target_columns(column_mappings, data_types, default_values, primary_keys),
        )
        transformed_df = self._apply_default_values(transformed_df, default_values)
        transformed_df = self._standardize_string_columns(transformed_df, data_types)
        transformed_df = self._standardize_phone_columns(transformed_df)
        transformed_df = self._standardize_postal_code_columns(transformed_df)
        transformed_df = self._normalize_boolean_columns(transformed_df, data_types)
        transformed_df = self._clean_currency_columns(transformed_df, data_types)
        transformed_df = self._standardize_datetime_columns(
            transformed_df,
            data_types=data_types,
            source_timezone=str(config.get("source_timezone", "UTC")),
        )
        transformed_df = self._cast_data_types(transformed_df, data_types)
        transformed_df = self._deduplicate(transformed_df, primary_keys)
        transformed_df = self._basic_validation(transformed_df)

        add_derived_columns = bool(config.get("add_derived_columns", True))
        if add_derived_columns:
            transformed_df = self._add_derived_columns(transformed_df, data_types)

        return transformed_df

    def read(self, spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
        """Read input data using standard Spark readers (no Delta-specific format required)."""

        source_type = str(config.get("source_type", "table")).strip().lower()

        if source_type == "table":
            source_table = str(config.get("source_table", "")).strip()
            if not source_table:
                raise ValueError("config['source_table'] must be set when source_type='table'.")
            return spark.table(source_table)

        source_path = str(config.get("source_path", "")).strip()
        if not source_path:
            raise ValueError("config['source_path'] must be set for non-table reads.")

        if source_type == "parquet":
            return spark.read.parquet(source_path)
        if source_type == "csv":
            return spark.read.option("header", "true").csv(source_path)
        if source_type == "json":
            return spark.read.json(source_path)

        LOGGER.warning("Unsupported source_type '%s'. Falling back to Spark parquet reader.", source_type)
        return spark.read.parquet(source_path)

    def write(self, df: DataFrame, config: Dict[str, Any]) -> None:
        """Write output data using standard Spark writers (no Delta-specific format required)."""

        mode = str(config.get("write_mode", "append")).strip().lower()
        target_table = str(config.get("target_table", "")).strip()
        target_path = str(config.get("target_path", "")).strip()
        target_format = str(config.get("target_format", "parquet")).strip().lower()

        if target_table:
            df.write.mode(mode).saveAsTable(target_table)
            return

        if target_path:
            if target_format in {"parquet", "csv", "json", "orc"}:
                df.write.mode(mode).format(target_format).save(target_path)
                return
            LOGGER.warning("Unsupported target_format '%s'. Falling back to parquet.", target_format)
            df.write.mode(mode).parquet(target_path)
            return

        LOGGER.warning("No target_table or target_path provided; skipping write.")

    def _load_mapping_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        mapping_path = str(config.get("mapping_config_path", "")).strip()
        if not mapping_path:
            LOGGER.warning("Missing config['mapping_config_path']; proceeding with empty mapping config.")
            return self._empty_mapping()

        try:
            with Path(mapping_path).open("r", encoding="utf-8") as file:
                loaded = json.load(file)
                return {
                    "column_mappings": loaded.get("column_mappings", {}),
                    "data_types": loaded.get("data_types", {}),
                    "default_values": loaded.get("default_values", {}),
                    "primary_keys": loaded.get("primary_keys", []),
                }
        except Exception as exc:  # noqa: BLE001 - defensive by design for production resilience.
            LOGGER.warning("Unable to load mapping config from '%s': %s", mapping_path, exc)
            return self._empty_mapping()

    @staticmethod
    def _empty_mapping() -> Dict[str, Any]:
        return {
            "column_mappings": {},
            "data_types": {},
            "default_values": {},
            "primary_keys": [],
        }

    @staticmethod
    def _target_columns(
        column_mappings: Dict[str, str],
        data_types: Dict[str, str],
        default_values: Dict[str, Any],
        primary_keys: Iterable[str],
    ) -> List[str]:
        return list(
            {
                *column_mappings.values(),
                *data_types.keys(),
                *default_values.keys(),
                *list(primary_keys),
            }
        )

    def _rename_columns(self, df: DataFrame, column_mappings: Dict[str, str]) -> DataFrame:
        renamed_df = df
        for source_col, target_col in column_mappings.items():
            if source_col in renamed_df.columns and source_col != target_col:
                renamed_df = renamed_df.withColumnRenamed(source_col, target_col)
            elif source_col not in renamed_df.columns:
                LOGGER.warning("Source column '%s' not found for rename to '%s'.", source_col, target_col)
        return renamed_df

    @staticmethod
    def _add_missing_columns(df: DataFrame, expected_columns: Iterable[str]) -> DataFrame:
        with_missing_df = df
        for column_name in expected_columns:
            if column_name not in with_missing_df.columns:
                with_missing_df = with_missing_df.withColumn(column_name, F.lit(None))
        return with_missing_df

    @staticmethod
    def _apply_default_values(df: DataFrame, default_values: Dict[str, Any]) -> DataFrame:
        applicable_defaults = {k: v for k, v in default_values.items() if k in df.columns}
        return df.fillna(applicable_defaults) if applicable_defaults else df

    def _standardize_string_columns(self, df: DataFrame, data_types: Dict[str, str]) -> DataFrame:
        standardized_df = df

        typed_string_columns = {
            col_name
            for col_name, dtype in data_types.items()
            if str(dtype).strip().lower() in {"string", "varchar", "char"}
        }
        schema_string_columns = {
            field.name for field in df.schema.fields if field.dataType.simpleString().startswith("string")
        }
        string_columns = typed_string_columns.union(schema_string_columns).intersection(set(df.columns))

        for column_name in string_columns:
            standardized_df = standardized_df.withColumn(
                column_name,
                F.lower(
                    F.regexp_replace(
                        F.trim(F.col(column_name).cast("string")),
                        r"[^a-zA-Z0-9\s\-_@\.]",
                        "",
                    )
                ),
            )

        return standardized_df

    @staticmethod
    def _standardize_phone_columns(df: DataFrame) -> DataFrame:
        phone_markers = ("phone", "mobile", "contact", "telephone")
        phone_columns = [c for c in df.columns if any(marker in c.lower() for marker in phone_markers)]

        standardized_df = df
        for column_name in phone_columns:
            standardized_df = standardized_df.withColumn(
                column_name,
                F.regexp_replace(F.col(column_name).cast("string"), r"[^0-9]", ""),
            )
        return standardized_df

    @staticmethod
    def _standardize_postal_code_columns(df: DataFrame) -> DataFrame:
        postal_markers = ("postal", "zip")
        postal_columns = [c for c in df.columns if any(marker in c.lower() for marker in postal_markers)]

        standardized_df = df
        for column_name in postal_columns:
            standardized_df = standardized_df.withColumn(
                column_name,
                F.upper(F.regexp_replace(F.col(column_name).cast("string"), r"\s+", "")),
            )
        return standardized_df

    def _normalize_boolean_columns(self, df: DataFrame, data_types: Dict[str, str]) -> DataFrame:
        boolean_markers = ("is_", "has_", "flag", "active", "enabled")
        boolean_columns = {
            col_name
            for col_name, dtype in data_types.items()
            if str(dtype).strip().lower() in {"boolean", "bool"}
        }
        boolean_columns = boolean_columns.union(
            {
                c
                for c in df.columns
                if any(c.lower().startswith(marker) for marker in boolean_markers)
                or c.lower().endswith("_flag")
            }
        )

        normalized_df = df
        true_values = ["y", "yes", "true", "1", "t"]
        false_values = ["n", "no", "false", "0", "f"]

        for column_name in boolean_columns.intersection(set(df.columns)):
            normalized_df = normalized_df.withColumn(
                column_name,
                F.when(F.lower(F.trim(F.col(column_name).cast("string"))).isin(true_values), F.lit(True))
                .when(F.lower(F.trim(F.col(column_name).cast("string"))).isin(false_values), F.lit(False))
                .otherwise(F.col(column_name).cast("boolean")),
            )

        return normalized_df

    def _clean_currency_columns(self, df: DataFrame, data_types: Dict[str, str]) -> DataFrame:
        currency_markers = ("amount", "price", "cost", "revenue", "sales")
        typed_numeric_columns = {
            col_name
            for col_name, dtype in data_types.items()
            if str(dtype).strip().lower() in {"double", "decimal", "float"}
        }

        candidate_columns = typed_numeric_columns.union(
            {c for c in df.columns if any(marker in c.lower() for marker in currency_markers)}
        )

        cleaned_df = df
        for column_name in candidate_columns.intersection(set(df.columns)):
            cleaned_df = cleaned_df.withColumn(
                column_name,
                F.regexp_replace(F.col(column_name).cast("string"), r"[$,]", "").cast("double"),
            )

        return cleaned_df

    def _standardize_datetime_columns(
        self,
        df: DataFrame,
        data_types: Dict[str, str],
        source_timezone: str,
    ) -> DataFrame:
        datetime_columns: List[Tuple[str, str]] = [
            (col_name, str(dtype).strip().lower())
            for col_name, dtype in data_types.items()
            if str(dtype).strip().lower() in {"date", "timestamp"}
        ]

        standardized_df = df
        for column_name, target_dtype in datetime_columns:
            if column_name not in standardized_df.columns:
                continue

            timestamp_col = F.to_timestamp(F.col(column_name).cast("string"))
            utc_col = F.to_utc_timestamp(timestamp_col, source_timezone)

            if target_dtype == "date":
                standardized_df = standardized_df.withColumn(column_name, utc_col.cast("date"))
            else:
                standardized_df = standardized_df.withColumn(column_name, utc_col)

        return standardized_df

    @staticmethod
    def _cast_data_types(df: DataFrame, data_types: Dict[str, str]) -> DataFrame:
        casted_df = df
        for column_name, target_type in data_types.items():
            if column_name in casted_df.columns:
                casted_df = casted_df.withColumn(column_name, F.col(column_name).cast(str(target_type)))
        return casted_df

    @staticmethod
    def _deduplicate(df: DataFrame, primary_keys: List[str]) -> DataFrame:
        valid_keys = [key for key in primary_keys if key in df.columns]
        if valid_keys:
            return df.dropDuplicates(valid_keys)
        return df.dropDuplicates()

    @staticmethod
    def _basic_validation(df: DataFrame) -> DataFrame:
        if "sales_amount" in df.columns:
            return df.filter((F.col("sales_amount").isNull()) | (F.col("sales_amount") >= F.lit(0)))
        return df

    @staticmethod
    def _add_derived_columns(df: DataFrame, data_types: Dict[str, str]) -> DataFrame:
        date_columns = [
            col_name
            for col_name, dtype in data_types.items()
            if str(dtype).strip().lower() in {"date", "timestamp"} and col_name in df.columns
        ]
        reference_column = date_columns[0] if date_columns else ("date" if "date" in df.columns else None)

        if not reference_column:
            return df

        return (
            df.withColumn("record_year", F.year(F.col(reference_column)))
            .withColumn("record_month", F.month(F.col(reference_column)))
        )
