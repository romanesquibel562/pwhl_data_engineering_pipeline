-- ======================================================================================================
-- 01_create_dataset.sql
-- Purpose: Create the dataset in BigQuery (for reference only)
-- This replicates what load_to_bq.py does programmatically. Included for workflow organization only.
-- ======================================================================================================

CREATE SCHEMA IF NOT EXISTS `lustrous-pivot-475720-n0.pwhl_takehome`
OPTIONS(
  location = 'US',
  default_table_expiration_days = 365
);
