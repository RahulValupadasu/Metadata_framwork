"""Utilities for loading source runtime configuration from Delta control tables."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


class ConfigNotFoundError(LookupError):
    """Raised when no active configuration exists for a source."""


class ConfigLoader:
    """Load active source configuration records from a Delta control table.

    Parameters
    ----------
    spark:
        Active :class:`SparkSession` used to query the control table.
    table_name:
        Fully-qualified Delta table name that stores source configurations.
    """

    def __init__(self, spark: SparkSession, table_name: str = "config.control_table") -> None:
        self.spark = spark
        self.table_name = table_name

    def load_config(self, source_name: str) -> Dict[str, Any]:
        """Fetch an active source configuration and return it as a dictionary.

        Parameters
        ----------
        source_name:
            Unique name of the data source whose active config is requested.

        Returns
        -------
        dict[str, Any]
            Source configuration represented as a native Python dictionary.

        Raises
        ------
        ValueError
            If ``source_name`` is blank.
        ConfigNotFoundError
            If no active configuration record exists for ``source_name``.
        """

        normalized_source_name = source_name.strip() if source_name else ""
        if not normalized_source_name:
            raise ValueError("source_name must be a non-empty string.")

        config_df = self._build_active_config_query(normalized_source_name)
        config_row = config_df.limit(1).collect()
        if not config_row:
            raise ConfigNotFoundError(
                f"No active configuration found in {self.table_name} for source_name='{normalized_source_name}'."
            )

        config = config_row[0].asDict(recursive=True)
        LOGGER.info(
            "Loaded active config for source_name='%s' from %s: %s",
            normalized_source_name,
            self.table_name,
            json.dumps(config, sort_keys=True, default=str),
        )
        return config

    def _build_active_config_query(self, source_name: str) -> DataFrame:
        """Construct the active configuration query DataFrame for a source."""

        return (
            self.spark.table(self.table_name)
            .filter((F.col("source_name") == source_name) & (F.col("active_flag") == F.lit(True)))
        )
