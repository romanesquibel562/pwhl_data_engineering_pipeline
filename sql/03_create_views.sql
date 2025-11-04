-- ======================================================================================================
-- 03_create_views.sql
-- Purpose: Create analysis views that join the curated fact table to dimensions
--          for easy, consistent EDA and downstream reporting.
-- Execution: Run in BigQuery after 02_create_dims_and_fact.sql has completed.
-- Notes:
--   - Primary analysis view: vw_sales_weather
--   - Convenience rollups:   vw_market_daily, vw_venue_section_daily
-- ======================================================================================================


-- =========================================================================================
-- vw_sales_weather
-- A single, tidy view for analysis that joins fact_ticket_sales to venue and weather dims.
-- Grain: event_date × venue_id × section
-- =========================================================================================
CREATE OR REPLACE VIEW `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather` AS
SELECT
  f.event_date,
  f.venue_id,
  v.venue,
  v.market,
  f.section,
  f.tickets_sold,
  f.revenue,
  f.avg_price,
  f.section_capacity,
  f.utilization,
  w.avg_temp_c,
  w.min_temp_c,
  w.max_temp_c,
  w.avg_rh_pct,
  w.avg_wind_mps,
  w.total_precip_mm,
  w.windy_hours,
  w.rainy_hours,
  w.freezing_hours,
  w.hours_observed
FROM `lustrous-pivot-475720-n0.pwhl_takehome.fact_ticket_sales` f
JOIN `lustrous-pivot-475720-n0.pwhl_takehome.dim_venue`   v USING (venue_id)
LEFT JOIN `lustrous-pivot-475720-n0.pwhl_takehome.dim_weather` w
  ON  w.market     = v.market
  AND w.event_date = f.event_date;



-- =========================================================================================
-- vw_market_daily
-- Daily rollup by market for quick KPI tracking (utilization and revenue).
-- Grain: event_date × market
-- =========================================================================================
CREATE OR REPLACE VIEW `lustrous-pivot-475720-n0.pwhl_takehome.vw_market_daily` AS
SELECT
  s.event_date,
  s.market,
  SUM(s.tickets_sold)                          AS tickets_sold,
  SUM(s.revenue)                               AS revenue,
  SAFE_DIVIDE(SUM(s.revenue), NULLIF(SUM(s.tickets_sold),0)) AS avg_price,
  AVG(s.utilization)                           AS avg_utilization,
  AVG(s.avg_temp_c)                            AS avg_temp_c,
  AVG(s.total_precip_mm)                       AS avg_total_precip_mm
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather` s
GROUP BY 1,2;



-- =========================================================================================
-- vw_venue_section_daily
-- Helpful for section-level performance tracking per venue.
-- Grain: event_date × venue_id × section
-- =========================================================================================
CREATE OR REPLACE VIEW `lustrous-pivot-475720-n0.pwhl_takehome.vw_venue_section_daily` AS
SELECT
  s.event_date,
  s.venue_id,
  s.venue,
  s.market,
  s.section,
  s.tickets_sold,
  s.revenue,
  s.avg_price,
  s.section_capacity,
  s.utilization,
  s.avg_temp_c,
  s.total_precip_mm
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather` s;