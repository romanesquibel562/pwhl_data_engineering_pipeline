-- =========================================================================================
-- 02_create_dims_and_fact.sql
-- Purpose: Build all dimension and fact tables for the PWHL Take-Home project.
-- Execution: Run in BigQuery after the dataset and raw table exist.
-- Notes:
--   - Uses source table: fact_ticket_sales_with_weather (loaded by load_to_bq.py)
--   - Creates the following tables:
--       • dim_venue
--       • dim_market
--       • dim_section
--       • dim_date
--       • dim_weather
--       • fact_ticket_sales
-- =========================================================================================


-- =========================================================================================
-- DIM_VENUE
-- Contains venue and market mapping for all events.
-- =========================================================================================
CREATE OR REPLACE TABLE `lustrous-pivot-475720-n0.pwhl_takehome.dim_venue` AS
SELECT DISTINCT
  venue_id,
  venue,
  market
FROM `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales_with_weather`;


-- =========================================================================================
-- DIM_MARKET
-- Simple list of unique markets (cities) in the dataset.
-- =========================================================================================
CREATE OR REPLACE TABLE `lustrous-pivot-475720-n0.pwhl_takehome.dim_market` AS
SELECT DISTINCT
  market AS market_name
FROM `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales_with_weather`
WHERE market IS NOT NULL;


-- =========================================================================================
-- DIM_SECTION
-- Contains seating section information per venue with capacity.
-- =========================================================================================
CREATE OR REPLACE TABLE `lustrous-pivot-475720-n0.pwhl_takehome.dim_section` AS
SELECT DISTINCT
  venue_id,
  section,
  section_capacity
FROM `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales_with_weather`
WHERE section IS NOT NULL;


-- =========================================================================================
-- DIM_DATE
-- Standard date dimension for the January–February 2025 analysis window.
-- =========================================================================================
CREATE OR REPLACE TABLE `lustrous-pivot-475720-n0.pwhl_takehome.dim_date` AS
SELECT
  day AS date_key,
  EXTRACT(YEAR FROM day) AS year,
  EXTRACT(MONTH FROM day) AS month,
  EXTRACT(DAY FROM day) AS day_of_month,
  FORMAT_DATE('%A', day) AS weekday_name,
  EXTRACT(ISOWEEK FROM day) AS iso_week
FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01','2025-02-28', INTERVAL 1 DAY)) AS day;


-- =========================================================================================
-- DIM_WEATHER
-- Daily market-level weather data for events (one row per market per date).
-- =========================================================================================
CREATE OR REPLACE TABLE `lustrous-pivot-475720-n0.pwhl_takehome.dim_weather`
PARTITION BY event_date
CLUSTER BY market AS
SELECT
  market,
  event_date,
  avg_temp_c,
  min_temp_c,
  max_temp_c,
  avg_rh_pct,
  avg_wind_mps,
  total_precip_mm,
  windy_hours,
  rainy_hours,
  freezing_hours,
  hours_observed
FROM `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales_with_weather`
WHERE event_date IS NOT NULL;


-- =========================================================================================
-- FACT_TICKET_SALES
-- Aggregated fact table: one row per event_date × venue_id × section.
-- Includes sales, revenue, and utilization metrics.
-- =========================================================================================
CREATE OR REPLACE TABLE `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales`
PARTITION BY DATE(event_date)
CLUSTER BY venue_id, section AS
SELECT
  event_date,
  venue_id,
  section,
  SUM(tickets_sold) AS tickets_sold,
  SUM(revenue) AS revenue,
  SAFE_DIVIDE(SUM(revenue), NULLIF(SUM(tickets_sold), 0)) AS avg_price,
  ANY_VALUE(section_capacity) AS section_capacity,
  SAFE_DIVIDE(SUM(tickets_sold), NULLIF(ANY_VALUE(section_capacity), 0)) AS utilization
FROM `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales_with_weather`
GROUP BY 1,2,3;

-- =========================================================================================
-- End of 02_create_dims_and_fact.sql   