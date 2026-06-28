# flight-delay-intelligence-platform
Real-time flight delay propagation system built on Databricks — Spark Structured Streaming, Delta Live Tables, Medallion Architecture

# ✈️ Real-time Flight Delay Intelligence Platform

> End-to-end data engineering pipeline built on **Databricks** that ingests live flight and weather data, processes it through a **Medallion Architecture**, detects cascade delay propagation, and serves insights via a real-time SQL dashboard.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│  OpenSky Network API  │  OpenWeather API  │  OpenFlights CSV │
│  (13,330 live flights)│  (21 airports)    │  (6,072 airports)│
└──────────┬────────────┴────────┬──────────┴────────┬─────────┘
           │                    │                   │
           ▼                    ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER · Delta Lake               │
│  bronze_flights      │  bronze_weather   │  bronze_rejected  │
│  13,213 raw records  │  21 airport rows  │  117 bad records  │
│                      │                   │  (Dead Letter Q)  │
└──────────────────────┴────────┬──────────┴───────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│              SILVER LAYER · Delta Lake · DLT Expectations   │
│                      silver_flights                         │
│  • 12,073 airborne flights                                  │
│  • Haversine nearest airport calculation                    │
│  • Delay scoring (velocity + altitude + vertical rate)      │
│  • Weather enrichment JOIN (delay_reason classification)    │
│  • DLT data quality checks (4 rules)                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
┌──────────────────┐    ┌───────────────────────┐
│  GOLD LAYER      │    │  GOLD LAYER            │
│  airline_summary │    │  cascade_impact        │
│  234 airlines    │    │  1,458 delayed flights │
│  delay % ranked  │    │  ripple effect scored  │
└────────┬─────────┘    └──────────┬─────────────┘
         │                         │
         └────────────┬────────────┘
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                     SERVING LAYER                           │
│  AI/BI Dashboard  │  Databricks Workflow  │  SQL Alert      │
│  6 live widgets   │  Every 6 hrs · 4 tasks│  Delay > 15%   │
└─────────────────────────────────────────────────────────────┘
         Orchestrated by Delta Live Tables · Unity Catalog
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Platform | Databricks (Serverless) |
| Language | Python · PySpark · SQL |
| Storage | Delta Lake · Unity Catalog |
| Pipeline | Delta Live Tables (DLT) |
| Orchestration | Databricks Workflows |
| Data Sources | OpenSky Network · OpenWeather · OpenFlights |
| Geospatial | Haversine distance formula (UDF) |
| Alerting | Databricks SQL Alerts |
| Visualization | Databricks AI/BI Dashboard |
| Version Control | Git · GitHub |

---

## 📁 Project Structure

```
flight-delay-intelligence-platform/
│
├── notebooks/
│   ├── 01_bronze_transformation.py   # Raw ingestion — OpenSky + OpenWeather
│   ├── 02_silver_transformation.py   # Enrichment — Haversine + delay scoring
│   ├── 03_gold_aggregation.py        # Aggregations — airline summary + cascade
│   ├── 04_dlt_pipeline.py            # Delta Live Tables — full pipeline
│   └── 05_ml_models.py               # MLflow experiments
│
├── data/
│   └── airports.dat                  # OpenFlights airport lookup (6,072 airports)
│
├── docs/
│   ├── architecture.png              # Pipeline architecture diagram
│   └── dashboard.png                 # Databricks SQL Dashboard screenshot
│
└── README.md
```

---

## 🚀 Pipeline Overview

### 1. Bronze Layer — Raw Ingestion
- Pulls **13,330 live flights** from OpenSky Network REST API
- Fetches **real-time weather** for 21 major airports (CDG, LHR, JFK, DXB, KHI, and more)
- Applies schema validation — invalid records routed to **`bronze_rejected`** (dead letter queue)
- All data stored as-is in **Delta format** for full auditability

### 2. Silver Layer — Enrichment & Scoring
- Filters to **12,073 airborne flights** (excludes on-ground)
- **Haversine UDF** calculates nearest airport from 6,072 global airports using lat/lon coordinates
- **Delay scoring logic** based on flight physics:
  - Holding pattern detection (APPROACH phase + vertical rate > -2 m/s)
  - Abnormal cruise speed (velocity < 150 m/s)
  - Low altitude anomaly (velocity < 100 m/s)
- **Weather JOIN** classifies delay reason: `WEATHER` · `HIGH_WINDS` · `UNKNOWN`
- **DLT Expectations** enforce 4 data quality rules — invalid rows quarantined

### 3. Gold Layer — Business Aggregations
- **`gold_airline_summary`** — 234 airlines ranked by delay %, avg delay minutes, total flights
- **`gold_cascade_impact`** — 1,458 delayed flights with ripple effect score (how many connecting flights affected at same airport)

### 4. Orchestration
- **Delta Live Tables** pipeline wires Bronze → Silver → Gold declaratively
- **Databricks Workflow** runs full pipeline every 6 hours automatically (4 tasks in sequence)
- **SQL Alert** fires email notification when system-wide delay rate crosses 15%

---

## 📊 Dashboard

Live **Databricks AI/BI Dashboard** with 6 widgets:

| Widget | Description |
|---|---|
| KPI counters | Total flights · Delayed · On-time · Avg delay mins |
| Top 10 airlines | Delay percentage ranking |
| Delayed by country | Geographic distribution |
| Flight phase distribution | CRUISE · APPROACH · LANDING · CLIMB |
| Cascade impact by airport | Top airports by ripple effect |
| Delayed flights table | Searchable — icao24 · callsign · airport · delay |

---

## 🔑 Key Engineering Decisions

**Why Haversine over country mapping?**
OpenSky provides lat/lon coordinates, not airport codes. A simple country → airport hardcode would cover ~6 countries. Haversine UDF against 6,072 airports gives accurate nearest airport for every flight globally.

**Why dead letter queue in Bronze?**
117 records had null ICAO codes or missing coordinates. Rather than silently dropping them, they're stored in `bronze_rejected` with a rejection reason — enables debugging and reprocessing.

**Why delay scoring instead of scheduled time comparison?**
OpenSky doesn't provide scheduled departure/arrival times. Delay is estimated from flight physics: holding patterns, abnormal speeds, and unusual descent rates — cross-referenced with live weather data.

**Why DLT over raw Spark jobs?**
DLT provides declarative pipeline definition, built-in data quality expectations, automatic retry, and lineage tracking — production-grade pipeline with minimal boilerplate.

---

## ⚡ Quickstart

### Prerequisites
- Databricks workspace (Serverless compute)
- OpenSky Network access (free, no API key required)
- OpenWeather API key (free tier)

### Setup
```python
# 1. Upload airports.dat to Unity Catalog Volume
# /Volumes/workspace/default/my_volume/airports.dat

# 2. Set your OpenWeather API key in 01_bronze_transformation.py
WEATHER_API_KEY = "your_key_here"

# 3. Run notebooks in order:
# 01 → 02 → 03 → 04

# 4. Create DLT pipeline pointing to 04_dlt_pipeline.py

# 5. Schedule via Databricks Workflow
```

---

## 📈 Sample Results

```
Total flights tracked:     12,073
Delayed flights:            1,508  (12.49%)
Average delay:              4.83 minutes

Top delayed airline:        CXK    (50.79% delay rate)
Most impacted airport:      CGZ    (340 potentially affected flights)
Most delayed country:       USA    (16.54% delay rate)
```

---

## 🗺️ Roadmap

- [ ] Dynamic weather fetching for all nearest airports (currently 21 major hubs)
- [ ] ML delay prediction model using Kaggle labeled flight dataset + MLflow
- [ ] SCD Type 2 on Silver layer for flight history tracking
- [ ] Streamlit app deployment via Databricks Apps
- [ ] AeroDataBox API integration for scheduled vs actual time comparison

---

## 🙏 Data Sources

| Source | Usage | License |
|---|---|---|
| [OpenSky Network](https://opensky-network.org/) | Live flight state vectors | Free · Research use |
| [OpenWeather API](https://openweathermap.org/) | Real-time airport weather | Free tier |
| [OpenFlights](https://openflights.org/data) | Airport coordinates lookup | Open Data |

---

*Built with Databricks Community Edition · PySpark · Delta Lake*
