# Airspace Intelligence
### Real-Time Aircraft Monitoring over the Iberian Peninsula
**Modern Data Architectures for Big Data II — IE University**

---

## Group 4 Members

| # | Name |
|---|------|
| 1 | Rincon Juan José |
| 2 | Gelin Romain |
| 3 | Gaitan Diego |
| 4 | Tcheishvili Luka |
| 5 | Tambey Cécile |

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Infrastructure Setup](#3-infrastructure-setup)
4. [Apache NiFi Pipeline](#4-apache-nifi-pipeline)
5. [Part I — Descriptive Analysis](#5-part-i--descriptive-analysis)
6. [Part II — Corridor Clustering Analysis](#6-part-ii--corridor-clustering-analysis)
7. [Part III — Corridor Deviation Classifier](#7-part-iii--corridor-deviation-classifier)
8. [Part IV — Real-Time Streaming Predictions](#8-part-iv--real-time-streaming-predictions)
9. [Key Results](#9-key-results)
10. [Conclusions](#10-conclusions)
11. [Future Work](#11-future-work)

---

## 1. Project Overview

Airspace Intelligence is a real-time Big Data pipeline that continuously ingests live aircraft telemetry from the **OpenSky Network API**, stores raw JSON snapshots in **MinIO** object storage, and performs multi-layered analysis using **Apache Spark** — from descriptive traffic statistics to unsupervised corridor discovery and real-time deviation detection.

| Field | Detail |
|-------|--------|
| Monitored Region | Iberian Peninsula (lat 35.5–44.5°N, lon -10.0–4.5°E) |
| Ingestion Frequency | Every 10 seconds |
| Data Source | OpenSky Network REST API |
| Spark Features Used | Structured Streaming · SQL · ML (BisectingKMeans, RandomForest) |
| Real-Time Inference | Kafka + Spark Structured Streaming |

---

## 2. Architecture

```
OpenSky Network API
        │
        ▼  (HTTP GET every 10 sec)
┌──────────────────┐
│   Apache NiFi    │  GenerateFlowFile → InvokeHTTP → PutS3Object
└──────────────────┘
        │
        ▼  (JSON snapshots ~49 KB each)
┌──────────────────────────────────────────────┐
│  MinIO — Bronze Layer                        │
│  Bucket: airspace-intelligence-bronze        │
│  Path:   opensky/bronze/YYYY-MM-DD/HH/*.json │
└──────────────────────────────────────────────┘
        │
        ▼  (s3a:// via hadoop-aws + aws-java-sdk-bundle)
┌──────────────────────────────────────────────┐
│  Apache Spark (PySpark) — Jupyter Notebook   │
│                                              │
│  Part I:  Descriptive Analysis               │
│           Spark SQL · Density Heatmap        │
│  Part II: Corridor Clustering                │
│           BisectingKMeans · Trajectory Eng.  │
│  Part III: Deviation Classifier              │
│           RandomForest · Model Persistence   │
│  Part IV: Streaming Inference                │
│           Kafka → Spark Streaming → RF Model │
└──────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────┐
│  Outputs              │
│  Seaborn · Matplotlib │
│  Folium (HTML maps)   │
│  Trained ML Models    │
│  (persisted to MinIO) │
└───────────────────────┘
```

---

## 3. Infrastructure Setup

### 3.1 MinIO Object Storage

```bash
minio server --address :9000 --console-address :9001 /data/s3
```

| Component | Endpoint |
|-----------|----------|
| S3 API | `http://127.0.0.1:9000` |
| Web Console | `http://127.0.0.1:9001` |
| Bronze Bucket | `airspace-intelligence-bronze` |
| Spark Bronze Path | `s3a://airspace-intelligence-bronze/opensky/bronze/` |

Create the bucket `airspace-intelligence-bronze` via the MinIO console before starting ingestion. Files are stored in dated subfolders (`opensky/bronze/YYYY-MM-DD/HH/`), which is why Spark requires `recursiveFileLookup=true` when reading.

### 3.2 Spark Session Configuration

```python
spark = (SparkSession.builder
    .appName("AirspaceIntelligence")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint",       "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())
```

---

## 4. Apache NiFi Pipeline

**Flow:** `GenerateFlowFile → InvokeHTTP → LogAttribute → PutS3Object`

| Processor | Purpose |
|-----------|---------|
| GenerateFlowFile | Triggers pipeline every 10 seconds |
| InvokeHTTP | GET request to OpenSky API with Iberian bounding box |
| LogAttribute | Logs FlowFile attributes for debugging |
| PutS3Object | Writes JSON snapshot to MinIO Bronze bucket |

The Object Key uses NiFi Expression Language to auto-create a date/hour folder hierarchy:
```
opensky/bronze/${now():format('yyyy-MM-dd')}/${now():format('HH')}/opensky_${now():format('yyyyMMdd_HHmmss_SSS')}.json
```

---

## 5. Part I — Descriptive Analysis

### 5.1 Traffic Analysis by Country & Airline (Spark SQL)

Aircraft DataFrames are registered as SQL temp views and queried for country-level and airline-level traffic breakdowns.

**Top countries by unique aircraft:**

| Country | Unique A/C | Market Share | Avg Speed (km/h) | Avg Alt (m) |
|---------|-----------|-------------|-----------------|------------|
| Spain | **141** | **26.4%** | 569 | 5,404 |
| Ireland | 57 | 10.7% | 779 | 9,548 |
| France | 52 | 9.7% | 602 | 7,076 |
| United Kingdom | 39 | 7.3% | 765 | 9,685 |
| Portugal | 38 | 7.1% | 527 | 5,182 |
| Morocco | 17 | 3.2% | **821** | **10,705** |

**Top airlines:**

| Code | Airline | Unique A/C | Avg Speed (km/h) |
|------|---------|-----------|-----------------|
| RYR | Ryanair | **83** | 756 |
| VLG | Vueling | 47 | 649 |
| EJU | easyJet Europe | 27 | 738 |
| TAP | TAP Air Portugal | 21 | 671 |
| ANE | Air Nostrum | 21 | 569 |
| RAM | Royal Air Maroc | 12 | **858** |

Key findings: Spain dominates at 26.4% with dense short-haul domestic operations. Ireland's 10.7% share is disproportionately high — driven by Ryanair's Irish registration. Morocco's aircraft fly fastest (821 km/h) and highest (10,705 m), indicating transatlantic overflights at cruising altitude.

### 5.2 Airspace Density Heatmap (Folium)

The monitored region is divided into a 1°×1° spatial grid. Each cell-snapshot pair counts unique aircraft, and congestion is flagged when a cell exceeds 5 aircraft.

| Metric | Value |
|--------|-------|
| Congestion rate | **89.5%** of cell-snapshot observations |
| Highest density corridor | NE–SW across central Spain |

The heatmap is rendered as an interactive Folium HeatMap on dark CartoDB tiles.

---

## 6. Part II — Corridor Clustering Analysis

### 6.1 Feature Engineering

Raw ADS-B observations are variable-length time series. To make them clusterable, each flight goes through:

1. **Flight segmentation** — each `icao24` track is split into individual flight legs using time-gap detection (gaps > threshold start a new segment)
2. **Flight summary statistics** — start/end position, altitude range, vertical rate profile per segment
3. **Airport proximity UDFs** — Haversine distance to major Iberian airports (LEMD, LEBL, LPPT, LEZL, LEMG, LEAL, etc.)
4. **Flight type classification** — each leg is classified as *arrival*, *departure*, or *pass-through* based on proximity to airports at start/end of flight
5. **Trajectory resampling** — variable-length trajectories are resampled to a fixed 15-waypoint (30-dimensional) vector for clustering

### 6.2 Bisecting K-Means Corridor Discovery

**BisectingKMeans** is applied separately per flight type to discover corridor structure:

| Flight Type | k | Silhouette Score |
|-------------|---|-----------------|
| Pass-through | 8 | **0.458** |
| Arrival | 5 | **0.819** |
| Departure | 5 | **0.904** |

Corridors are labeled with semantic identifiers:
- Pass-through: `PT-0`, `PT-1`, ..., `PT-7`
- Arrivals: `ARR-LEMD-2`, `ARR-LEBL-4`, `ARR-LPPT-0`, `ARR-LEZL-1`, `ARR-LEAL-3`
- Departures: `DEP-LEBL-4`, `DEP-LEMD-2`, `DEP-LEMG-1`, `DEP-LPPT-0`, `DEP-LEAL-3`

Results are visualized on interactive Folium maps with distinct color palettes per corridor.

---

## 7. Part III — Corridor Deviation Classifier

### 7.1 Random Forest Classifier

With corridor labels from BKMeans, a **Random Forest classifier** is trained to predict corridor membership from instantaneous observable features (position, altitude, speed, heading).

The operational goal: if we know what corridor an aircraft *should* be in, we can detect when it **deviates** — useful for air traffic control anomaly detection.

| Metric | Value |
|--------|-------|
| Train snapshots | **37,108** |
| Test snapshots | **6,096** |
| Train flights / Test flights | 395 / 66 (no overlap) |
| F1 Score | **0.679** |
| Accuracy | **0.689** |
| Weighted Precision | **0.709** |
| Weighted Recall | **0.689** |

### 7.2 Deviation Flagging

Each prediction receives a deviation status based on the model's class probability:

| Status | Condition | Test Count |
|--------|-----------|-----------|
| On corridor | probability ≥ 0.70 | **3,963** |
| Marginal | 0.50 ≤ probability < 0.70 | **1,482** |
| Deviating | probability < 0.50 | **651** |

### 7.3 Model Persistence

The trained pipeline (StringIndexer → VectorAssembler → RandomForest → IndexToString) is saved to MinIO at `s3a://airspace-intelligence-bronze/models/corridor_classifier/` for reuse in streaming inference.

---

## 8. Part IV — Real-Time Streaming Predictions

The saved Random Forest model is loaded in a separate Spark Streaming session that consumes live data from **Apache Kafka**.

**Pipeline:**
```
Kafka Topic → Spark Structured Streaming → Parse OpenSky JSON
            → Apply Feature Engineering → Load RF Model
            → Predict Corridor + Deviation Status → Console Sink
```

This closes the loop: the batch-trained model is deployed for real-time corridor classification and deviation detection on live ADS-B telemetry.

---

## 9. Key Results

| Metric | Value |
|--------|-------|
| Total aircraft records processed | **23,579** |
| Unique aircraft detected (1 hour) | **535** distinct ICAO24 codes |
| Average aircraft per snapshot | **352.8** |
| Airspace congestion rate | **89.5%** |
| Dominant operator | Ryanair — **83 unique aircraft** |
| Corridors discovered | **18** (8 pass-through, 5 arrival, 5 departure) |
| RF classifier accuracy | **0.689** |
| On-corridor prediction rate | **65%** of test observations |

---

## 10. Conclusions

1. **End-to-end pipeline validated:** NiFi → MinIO → Spark → ML → Streaming, running on a local OSBDET environment.
2. **Five distinct Spark capabilities exercised:** Structured Streaming, SQL, ML (BisectingKMeans), ML (RandomForest), and Kafka Streaming inference.
3. **Iberian airspace is structurally congested:** 89.5% congestion rate confirms this is baseline behaviour, not a peak-hour anomaly.
4. **Low-cost carrier dominance is visible in raw telemetry:** Ryanair alone represents ~15% of all unique aircraft detected.
5. **Corridor structure is real and discoverable:** BKMeans produces high-quality clusters (silhouette 0.90 for departures) that align with known air traffic geography.
6. **Real-time deviation detection is feasible:** The RF model generalizes to unseen flights with 0.689 accuracy, and the Kafka streaming pipeline enables live inference.

---

## 11. Future Work

- **24-hour ingestion** to capture full daily traffic cycles and morning vs. evening peak comparison
- **Silver/Gold layers** with Spark transformations for nearest-airport enrichment and Cassandra/PostgreSQL persistence
- **Apache Superset** dashboard connected to Gold layer for real-time operational monitoring
- **Weather + NOTAM integration** to reduce false positive deviation alerts from legitimate re-routing
- **Finer spatial grid** (0.25°) to better resolve local hotspots near major airports
- **GraphFrames** for airport-to-airport route modeling and hub connectivity analysis
- **Anomaly detection** using Spark ML Isolation Forest on velocity/altitude deviations

---

*Airspace Intelligence — IE University | Modern Data Architectures for Big Data II | March 2026*
