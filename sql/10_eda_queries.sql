-- ======================================================================================================
-- 10_eda_queries.sql
-- Purpose: Perform exploratory data analysis (EDA) on the curated BigQuery tables.
-- Author: Roman Esquibel
-- Execution: Run in BigQuery after 03_create_views.sql has completed.
-- Notes:
--   - Queries reference the vw_sales_weather, vw_market_daily, and vw_venue_section_daily views.
--   - Each section answers a distinct business question for the PWHL dataset.
-- ======================================================================================================


-- =========================================================================================
-- 1. Overview: Event counts by market and venue
--    Helps validate data coverage and ensure every market/venue combination loaded correctly.
-- =========================================================================================
SELECT
  market,
  venue,
  COUNT(DISTINCT event_date) AS num_events,
  SUM(tickets_sold)          AS total_tickets,
  SUM(revenue)               AS total_revenue,
  ROUND(AVG(utilization)*100,2) AS avg_utilization_pct
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`
GROUP BY market, venue
ORDER BY market, venue;



-- =========================================================================================
-- 2. Weather impact: Rainy vs Dry Days
--    Quantifies how precipitation influences utilization and revenue.
-- =========================================================================================
SELECT
  IF(total_precip_mm > 0, 'Rainy', 'Dry') AS weather_type,
  COUNT(*)                                AS num_events,
  ROUND(AVG(utilization)*100,2)           AS avg_utilization_pct,
  ROUND(AVG(revenue),2)                   AS avg_revenue,
  ROUND(AVG(avg_price),2)                 AS avg_ticket_price
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`
GROUP BY weather_type
ORDER BY weather_type;



-- =========================================================================================
-- 3. Temperature bands vs. utilization
--    Groups events into temperature ranges to examine comfort-related attendance effects.
-- =========================================================================================
WITH temp_bands AS (
  SELECT *,
    CASE
      WHEN avg_temp_c IS NULL THEN 'Unknown'
      WHEN avg_temp_c < 0 THEN '<0°C'
      WHEN avg_temp_c BETWEEN 0 AND 5  THEN '0–5°C'
      WHEN avg_temp_c BETWEEN 6 AND 10 THEN '6–10°C'
      WHEN avg_temp_c BETWEEN 11 AND 15 THEN '11–15°C'
      WHEN avg_temp_c BETWEEN 16 AND 20 THEN '16–20°C'
      ELSE '>20°C'
    END AS temp_band
  FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`
)
SELECT
  temp_band,
  COUNT(*) AS events,
  ROUND(AVG(utilization)*100,2) AS avg_utilization_pct,
  ROUND(AVG(revenue),2)         AS avg_revenue,
  ROUND(AVG(avg_price),2)       AS avg_ticket_price
FROM temp_bands
GROUP BY temp_band
ORDER BY temp_band;



-- =========================================================================================
-- 4. Correlation analysis between utilization/revenue and weather
--    Uses BigQuery's CORR() to quantify relationships.
-- =========================================================================================
SELECT
  CORR(utilization, avg_temp_c)      AS corr_util_temp,
  CORR(utilization, total_precip_mm) AS corr_util_precip,
  CORR(revenue,     avg_temp_c)      AS corr_rev_temp,
  CORR(revenue,     total_precip_mm) AS corr_rev_precip
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`;



-- =========================================================================================
-- 5. Section performance: identify high-demand areas
--    Compares utilization and pricing by section to spot premium or under-performing areas.
-- =========================================================================================
SELECT
  market,
  section,
  COUNT(*)                          AS num_events,
  ROUND(AVG(utilization)*100,2)     AS avg_utilization_pct,
  ROUND(AVG(avg_price),2)           AS avg_price,
  ROUND(AVG(revenue),2)             AS avg_revenue
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`
GROUP BY market, section
HAVING num_events >= 3
ORDER BY avg_utilization_pct DESC
LIMIT 20;



-- =========================================================================================
-- 6. Market-level summary
--    Provides top-level KPIs for each city/market across the sample period.
-- =========================================================================================
SELECT
  market,
  COUNT(DISTINCT event_date)            AS num_event_days,
  SUM(tickets_sold)                     AS total_tickets,
  ROUND(SUM(revenue),2)                 AS total_revenue,
  ROUND(AVG(utilization)*100,2)         AS avg_utilization_pct,
  ROUND(AVG(avg_temp_c),1)              AS avg_temp_c,
  ROUND(AVG(total_precip_mm),1)         AS avg_precip_mm
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`
GROUP BY market
ORDER BY avg_utilization_pct DESC;



-- =========================================================================================
-- 7. Near-sellout frequency
--    Determines how often sections reached 95%+ of capacity.
-- =========================================================================================
SELECT
  market,
  venue,
  COUNTIF(utilization >= 0.95) AS near_sellout_events,
  COUNT(*)                     AS total_events,
  ROUND(COUNTIF(utilization >= 0.95)/COUNT(*)*100,2) AS pct_near_sellout
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`
GROUP BY market, venue
ORDER BY pct_near_sellout DESC;



-- =========================================================================================
-- 8. Cross-check: ticket price vs utilization correlation
--    Tests price elasticity indications at section level.
-- =========================================================================================
SELECT
  CORR(utilization, avg_price) AS corr_util_price
FROM `lustrous-pivot-475720-n0.pwhl_takehome.vw_sales_weather`;

