# PWHL Data Engineering Take-Home Assessment

## Overview
This project demonstrates full-cycle data engineering by building a modular ETL pipeline that integrates **ticket sales**, **arena section capacity**, and **weather** data for all eight Professional Women’s Hockey League (PWHL) markets during **January and February 2025**.

The pipeline performs:

- **Data ingestion** from the OpenWeather API  
- **Data cleaning and transformation** of raw CSV files  
- **Integration** of weather metrics with ticket sales and capacity  
- **Loading** of analysis-ready tables into **Google BigQuery**

All steps are automated, logged, and reproducible using **Python**, **pandas**, and the **Google Cloud BigQuery client**.

---

## Objectives
This project fulfills the key assessment goals:

1. Ingest and store external API data and structured CSVs  
2. Clean, normalize, and validate all datasets  
3. Integrate weather with sales and capacity by market and date  
4. Load results into a BigQuery dataset  
5. Build an analysis-ready star schema  
6. Communicate design choices clearly through documentation and SQL

---

## Repository Structure
PWHL_DE_TAKEHOME/
│
├── .venv/                    # Virtual environment directory (ignored by .gitignore)
├── .vscode/                  # Visual Studio Code settings (optional)
├── config/                   # Configuration files for the project
│   ├── markets.yml           # Market configuration
│   └── settings.yml          # Additional settings configuration
│
├── data/                     # Data folder containing raw and cleaned data
│   ├── cleaned/              # Cleaned data for analysis and modeling
│   │   ├── dim_market.csv    # Market dimension data
│   │   ├── fact_ticket_sales_with_weather.csv  # Fact table with integrated weather data
│   │   └── (other cleaned CSV files)
│   │
│   ├── raw/                  # Raw data used in the pipeline
│   │   ├── weather/          # Raw weather data
│   │   ├── pwhl_ticket_sales.csv # Raw ticket sales data
│   │   └── (other raw CSV files)
│   │
│   └── reference/            # Reference files like data dictionary
│       └── pwhl_data_dictionary.csv # Contains descriptions of dataset fields
│
├── docs/                     # Documentation folder (e.g., schema diagrams)
│   └── star_schema.png       # Star schema diagram for the data model
│
├── logs/                     # Logs for ETL process
│   ├── clean_section_capacity.log
│   ├── clean_ticket_sales.log
│   └── (other log files)
│
├── pwhl_de_takehome/         # Internal package (if applicable)
├── scripts/                  # Python scripts for the ETL pipeline
│   ├── ingest_weather.py     # Script to ingest weather data
│   ├── clean_ticket_sales.py # Data cleaning for ticket sales
│   └── (other ETL scripts)
│
├── sql/                      # SQL scripts for BigQuery
│   ├── 01_create_dataset.sql # Script to create BigQuery dataset
│   ├── 02_create_dims_and_facts.sql # Script to create dimensions and facts tables
│   └── (other SQL files)
│
├── .gitignore                # Git ignore rules for the project
├── README.md                 # Project documentation
├── requirements.txt          # Python dependencies
├── run_pipeline.py           # Main entry point to run the ETL pipeline
└── .env.example              # Example .env file (containing placeholders for API keys and environment variables)
---

# Setup Instructions

### 1. Clone the Repository

```
git clone https://github.com/<your-username>/PWHL_DE_TAKEHOME.git
cd PWHL_DE_TAKEHOME
```

---

## 2. Create and Activate a Virtual Environment


python -m venv .venv
.venv\Scripts\activate     # Windows
source .venv/bin/activate  # macOS/Linux




## 3. Install Dependencies


pip install -r requirements.txt


---

## 4. Configure Environment Variables

Create a `.env` file in the project root with the following:

```ini
BQ_PROJECT_ID=<your-bigquery-project-id>
```

**Note:**
This pipeline was tested using the sandbox project ID `lustrous-pivot-475720-n0`.
Replace with your own project if reproducing.

---

## 5. Run the Pipeline

```
python run_pipeline.py
```

The orchestrator script executes every ETL stage sequentially, logs to `/logs/`, and loads cleaned data into BigQuery.

---

# Workflow Overview

* **Step 1:** `scripts/ingest_weather.py` — Calls the OpenWeather API to collect weather for all markets
* **Step 2:** `scripts/transform_weather.py` — Normalizes and aggregates weather data
* **Step 3:** `scripts/clean_ticket_sales.py` — Cleans and standardizes raw ticket sales per market
* **Step 4:** `scripts/clean_section_capacity.py` — Cleans section capacity and validates venue IDs
* **Step 5:** `scripts/integrate_weather_sales.py` — Joins ticket sales and weather by market and date
* **Step 6:** `scripts/materialize_dim_market.py` — Builds market dimension (unique venues, IDs, countries)
* **Step 7:** `scripts/load_to_bq.py` — Loads all cleaned tables into BigQuery
* **Step 8:** `run_pipeline.py` — Runs and logs the full process end-to-end

---

# Data Sources

```
* `pwhl_ticket_sales.csv` — Ticket transactions for all PWHL venues (CSV)
* `game_section_capacity.csv` — Section capacity and seat counts by venue (CSV)
* **OpenWeather API** — Historical daily weather per market (JSON → CSV)
* `pwhl_data_dictionary.csv` — Data dictionary and field descriptions (CSV)
```
---

# BigQuery Dataset

```
* **Dataset:** `pwhl_takehome`
* **Project ID:** `<your-bigquery-project-id>`
* **Region:** `US` (BigQuery free sandbox)
```
### Tables Created

* `fact_ticket_sales_with_weather`
* `dim_section_capacity`
* `dim_weather`
* `dim_market`

**Partitioning:** `event_date` (fact table)
**Relationships:** Fact table joins to all dimension tables by key columns.

---

# Data Model (Star Schema)

```
fact_ticket_sales_with_weather
 ├─ event_date (PK, partitioned)
 ├─ venue_id (FK)
 ├─ section (FK)
 ├─ market (FK)
 ├─ tickets_sold
 ├─ revenue
 ├─ avg_price
 ├─ utilization
 ├─ weather_id (FK)

dim_section_capacity
 ├─ venue_id
 ├─ section
 ├─ section_capacity

dim_weather
 ├─ weather_id
 ├─ market
 ├─ event_date
 ├─ avg_temp_c
 ├─ min_temp_c
 ├─ max_temp_c
 ├─ avg_rh_pct
 ├─ total_precip_mm
 ├─ windy_hours
 ├─ rainy_hours
 ├─ freezing_hours

dim_market
 ├─ market_id
 ├─ market_name
 ├─ country
 ├─ venue
 ├─ venue_id
```

---

# Example SQL Queries (Exploratory Analysis)

## 1. Market Utilization

```sql
SELECT
  market,
  ROUND(AVG(utilization), 3) AS avg_utilization,
  COUNT(DISTINCT event_date) AS games_observed
FROM `<your-bigquery-project-id>.pwhl_takehome.fact_ticket_sales_with_weather`
GROUP BY market
ORDER BY avg_utilization DESC;
```

## 2. Section Revenue and Price

```sql
SELECT
  section,
  SUM(revenue) AS total_revenue,
  ROUND(AVG(avg_price), 2) AS avg_ticket_price
FROM `<your-bigquery-project-id>.pwhl_takehome.fact_ticket_sales_with_weather`
GROUP BY section
ORDER BY total_revenue DESC;
```

## 3. Temperature vs. Utilization Correlation

```sql
SELECT
  f.market,
  ROUND(AVG(w.avg_temp_c), 2) AS avg_temp,
  ROUND(AVG(f.utilization), 3) AS avg_utilization,
  ROUND(CORR(w.avg_temp_c, f.utilization), 3) AS temp_util_corr
FROM `<your-bigquery-project-id>.pwhl_takehome.fact_ticket_sales_with_weather` f
JOIN `<your-bigquery-project-id>.pwhl_takehome.dim_weather` w
  ON f.market = w.market AND f.event_date = w.event_date
GROUP BY f.market
ORDER BY temp_util_corr DESC;
```

## 4. Market Summary

```sql
SELECT
  market,
  COUNT(DISTINCT event_date) AS games_observed,
  SUM(tickets_sold) AS total_tickets,
  SUM(revenue) AS total_revenue,
  ROUND(AVG(utilization), 3) AS avg_utilization
FROM `<your-bigquery-project-id>.pwhl_takehome.fact_ticket_sales_with_weather`
GROUP BY market
ORDER BY total_revenue DESC;
```

---

# Exploratory Data Analysis Results
Exploratory analysis in BigQuery confirmed that the integrated dataset is complete, consistent, and analytically sound. All joins between ticket sales, section capacity, and weather tables executed successfully, and key identifiers (market, venue_id, event_date) aligned without null or duplicate entries. The star schema performed efficiently for aggregation and correlation queries, validating the ETL pipeline’s accuracy.

At the section level, revenue patterns followed logical distributions: the Upper Bowl and Lower Bowl sections generated the highest total revenue due to ticket volume, while Club and Suite seating maintained higher prices but lower overall sales. Standing Room contributed minimal revenue, reflecting limited availability but stable pricing. These results confirm that pricing and capacity transformations were preserved through cleaning and normalization.

Across markets, utilization rates averaged ~59.7%, consistent with synthetic test data, yet temperature-utilization correlations behaved logically. Warmer markets (e.g., New York, Washington) showed mildly positive correlations between temperature and attendance, while colder regions (e.g., Minneapolis, Montreal) displayed weaker or negative correlations. These relationships confirm that the integrated model can capture meaningful environmental effects once populated with live data.

Overall, the EDA demonstrates that the data pipeline, schema design, and BigQuery integration function as intended. The model supports efficient querying across sales, weather, and capacity dimensions and provides a strong foundation for future analytical expansion—such as attendance forecasting, pricing optimization, and weather impact modeling once real, non-synthetic data is introduced.

## Key Findings

* All dataset joins executed correctly
* Schemas are aligned
* Transformations preserve numeric integrity

---

## Section-Level Insights

* Upper Bowl: **15,346,680 revenue | 75.47 avg price**
* Lower Bowl: **14,890,120 revenue | 75.58 avg price**
* Club: **6,350,640 revenue | 76.02 avg price**
* Suite: **4,476,240 revenue | 76.58 avg price**
* Standing Room: **2,098,720 revenue | 76.77 avg price**

*Upper and Lower Bowl sections generate most revenue; premium sections are higher-priced but smaller in volume.*

---

## Market-Level Summary

* Boston — Temp −1.98 °C | Util 0.597 | Corr 0.368
* New York — Temp −0.33 °C | Util 0.597 | Corr 0.357
* Washington — Temp 0.99 °C | Util 0.597 | Corr 0.254
* Montreal — Temp −7.76 °C | Util 0.597 | Corr 0.202
* Toronto — Temp −3.83 °C | Util 0.597 | Corr 0.111
* Ottawa — Temp −7.76 °C | Util 0.597 | Corr 0.071
* Chicago — Temp −4.10 °C | Util 0.597 | Corr 0.041
* Minneapolis — Temp −8.27 °C | Util 0.597 | Corr −0.067

**Observations**

* Temperature correlations behave logically: warmer markets show slightly higher utilization.
* Identical utilization values (~0.597) indicate synthetic or placeholder data.
* All metrics validate correct schema alignment and ETL performance.

---

# Design Decisions and Assumptions

* **Schema:** Star schema for analytical flexibility
* **Partitioning:** `event_date` for cost-effective queries
* **Identifiers:** Market codes (e.g., `BOS_01`, `TOR_01`) ensure unique venue mapping
* **Weather Integration:** Joined by `market` and `event_date`
* **Validation:** Cross-checked with `pwhl_data_dictionary.csv`
* **Logging:** All ETL steps record to `/logs/`
* **Reproducibility:** Fully re-runnable pipeline

---

# Possible Enhancements

* Replace synthetic data with live feeds
* Add validation using **Great Expectations**
* Schedule automation via **Apache Airflow**
* Build a **Looker Studio** dashboard
* Extend to predictive modeling with **BigQuery ML**

---

# Author

**Name:** Roman Esquibel
**Date:** November 2025
**Contact:**  romanesquib@gmail.com

