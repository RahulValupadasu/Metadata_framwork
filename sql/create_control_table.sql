-- Ensure the config schema exists
CREATE SCHEMA IF NOT EXISTS config;

-- Create a lightweight control table for execution metadata only
CREATE TABLE IF NOT EXISTS config.control_table (
  source_name STRING COMMENT 'Unique identifier for each data source (one row per source).',
  source_type STRING COMMENT 'Type of source system, such as csv, api, db, or pdf.',
  source_path STRING COMMENT 'Path, URL, or connection location where source data is read from.',
  target_table STRING COMMENT 'Destination Delta table to be loaded by the pipeline.',
  load_type STRING COMMENT 'Load strategy: full or incremental.',
  watermark_column STRING COMMENT 'Column used as watermark for incremental processing.',
  last_processed_value STRING COMMENT 'Last processed watermark value stored as string for flexibility.',
  mapping_config_path STRING COMMENT 'Path to external JSON mapping configuration file.',
  transformation_module STRING COMMENT 'Optional transformation plugin/module name applied before write.',
  active_flag BOOLEAN COMMENT 'Whether this pipeline/source configuration is active.',
  config_version STRING COMMENT 'Version tag of the configuration record for change tracking.',
  CONSTRAINT pk_control_table_source_name PRIMARY KEY (source_name)
)
USING DELTA
COMMENT 'Execution control metadata for Databricks pipelines; excludes schema-level metadata.';
