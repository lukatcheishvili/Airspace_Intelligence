# Airspace Intelligence
### Real-Time Aircraft Monitoring over the Iberian Peninsula
**Modern Data Architectures for Big Data II — IE University**

---

## Group 4 Members

| # | Name | Role |
|---|------|------|
| 1 | Rincon Juan José
| 2 | Gelin Romain
| 3 | Gaitan Diego
| 4 | Tcheishvili Luka
| 5 | Tambey Cécile

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Access Credentials](#3-access-credentials-confidential)
4. [MinIO Setup](#4-minio-setup)
5. [Apache NiFi Pipeline](#5-apache-nifi-pipeline)
6. [Jupyter Notebook — Spark Analysis](#6-jupyter-notebook--spark-analysis)
7. [Insights & Results](#7-insights--results)
8. [Conclusions](#8-conclusions)
9. [Future Work](#9-future-work)

---

## 1. Project Overview

Airspace Intelligence is a real-time Big Data pipeline that continuously ingests live aircraft telemetry from the **OpenSky Network API**, stores raw JSON snapshots in **MinIO** object storage, and performs analytical use cases using **Apache Spark**.

| Field | Detail |
|-------|--------|
| Monitored Region | Iberian Peninsula (lat 36.4–43.2°N, lon -10.0–2.5°E) |
| Ingestion Frequency | Every 10 seconds |
| Data Source | OpenSky Network REST API |
| Spark Features Used | SQL · MLlib (BisectingKMeans, RandomForestClassifier) · Folium |
| Submission Deadline | March 19th, 2026 via Blackboard |
| GitHub | https://github.com/lukatcheishvili/Airspace_Intelligence.git |

---

## 2. Architecture

```
OpenSky Network API
        │
        ▼  (HTTP GET every 10 sec)
┌──────────────────┐
│   Apache NiFi    │  GenerateFlowFile → InvokeHTTP → LogAttribute → PutS3Object
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
│  Part I  — Descriptive Analysis              │
│    · Traffic by Country & Airline (SQL)      │
│    · Airspace Density Heatmap (Folium)       │
│                                              │
│  Part II — Corridor Clustering               │
│    · Flight Segmentation & Feature Eng.      │
│    · BisectingKMeans Corridor Discovery      │
│    · Deviation Labelling                     │
│                                              │
│  Part III — Corridor Deviation Classifier    │
│    · RandomForest Classifier                 │
│    · Real-time corridor prediction           │
└──────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────┐
│  Visualisations       │
│  Seaborn · Matplotlib │
│  Folium (HTML maps)   │
└───────────────────────┘
```

---

## 3. Access Credentials *(Confidential)*

> **For team members and professor only. Do not share publicly.**

Credentials are loaded from a `.env` file (never committed to GitHub). A `.env.example` template is provided in the repository.

| Credential | Value |
|-----------|-------|
| MinIO Access Key | `airspace-intel` |
| MinIO Secret Key | `Confidential!` |
| MinIO S3 Endpoint | `http://127.0.0.1:9000` |
| MinIO Web Console | `http://127.0.0.1:9001` |
| Bronze Bucket | `airspace-intelligence-bronze` |
| Spark Bronze Path | `s3a://airspace-intelligence-bronze/opensky/bronze/` |
| OpenSky API | `https://opensky-network.org/api/states/all` |
| Bounding Box | `lamin=35.5 lomin=-10.0 lamax=44.5 lomax=4.5` |

### Security — .env setup

Create a `.env` file in the project root (never commit it):

```
MINIO_ACCESS_KEY=airspace-intel
MINIO_SECRET_KEY=Confidential!
MINIO_ENDPOINT=http://127.0.0.1:9000
```

Ensure `.gitignore` contains: `.env`

---

## 4. MinIO Setup

### 4.1 Start the Server

```bash
minio server --address :9000 --console-address :9001 /data/s3
```

| Flag | Purpose |
|------|---------| 
| `--address :9000` | S3 API port — used by NiFi and Spark |
| `--console-address :9001` | Web UI port — open in browser to manage buckets |
| `/data/s3` | Physical directory where all bucket data is stored |

### 4.2 Verify

```bash
sudo ss -tulnp | grep 9000   # confirm port is listening
sudo service minio status    # confirm service is active
```

Then open `http://127.0.0.1:9001` in a browser and log in with the credentials above.

### 4.3 Create the Bronze Bucket

1. In the MinIO console, click **Buckets** in the left sidebar
2. Click **Create Bucket**
3. Enter name exactly: `airspace-intelligence-bronze`
4. Leave all other settings as default — Access will show as **PRIVATE** (this is correct)
5. Click **Create Bucket**

---

## 5. Apache NiFi Pipeline

**Flow order:** `GenerateFlowFile → InvokeHTTP → LogAttribute → PutS3Object`

> Start MinIO **before** starting NiFi. There are **4 processors** in the pipeline.

### 5.1 Processor 1 — GenerateFlowFile

| Setting | Value |
|---------|-------|
| Run Schedule | `10 sec` |
| File Size | `0 b` |
| Batch Size | `1` |

### 5.2 Processor 2 — InvokeHTTP

| Property | Value |
|----------|-------|
| HTTP Method | `GET` |
| Remote URL | `https://opensky-network.org/api/states/all?lamin=35.5&lomin=-10.0&lamax=44.5&lomax=4.5` |
| Connection Timeout | `10 sec` |
| Read Timeout | `30 sec` |

Connect `Response` relationship to LogAttribute. Auto-terminate all others.

### 5.3 Processor 3 — LogAttribute

| Property | Value |
|----------|-------|
| Log Level | `info` |
| Log Payload | `false` |

Connect `success` relationship to PutS3Object.

### 5.4 Processor 4 — PutS3Object

| Property | Value |
|----------|-------|
| Bucket | `airspace-intelligence-bronze` |
| Object Key | `opensky/bronze/${now():format('yyyy-MM-dd')}/${now():format('HH')}/opensky_${now():format('yyyyMMdd_HHmmss_SSS')}.json` |
| Region | `us-east-1` |
| Access Key ID | `airspace-intel` |
| Secret Access Key | `Confidential!` |
| Endpoint Override URL | `http://127.0.0.1:9000` |
| Use Path Style Access | `True` |

---

## 6. Jupyter Notebook — Spark Analysis

### 6.1 SparkSession Configuration

The S3A connector is configured at runtime via `_jsc.hadoopConfiguration()` — credentials are loaded from environment variables:

```python
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

spark = SparkSession.builder.appName('AirspaceIntelligence').getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint",          MINIO_ENDPOINT)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",        MINIO_ACCESS_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",        MINIO_SECRET_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

### 6.2 Critical Data Load Pattern

```python
raw_df = (
    spark.read
    .schema(opensky_schema)
    .option('multiLine', True)
    .option('recursiveFileLookup', 'true')   # REQUIRED — files are in dated subfolders
    .json(BRONZE_PATH)
)
```

### 6.3 Bounding Box Filter

The notebook filters aircraft to a **tighter bounding box** around the Iberian Peninsula after loading:

```python
aircraft_df = aircraft_df
    .filter(F.col("latitude").between(36.4, 43.2))
    .filter(F.col("longitude").between(-10.0, 2.5))
```

### 6.4 Part I — Descriptive Analysis

**Traffic by Country & Airline (Spark SQL)**

- Registers `aircraft_df` as a SQL view: `aircraft_df.createOrReplaceTempView('aircraft')`
- Top-20 countries by unique aircraft with average speed and altitude
- Airline code extracted from first 3 uppercase characters of callsign
- Market share % per country computed against total unique aircraft

**Airspace Density Heatmap (Folium)**

- 1°×1° grid cells (upgraded from 2°×2°)
- `log1p` transform applied to aircraft counts to reduce visual skew from hotspots
- Interactive `HeatMap` layer on CartoDB dark_matter tiles
- Cyan grid rectangles drawn for spatial reference

### 6.5 Part II — Corridor Clustering Analysis

**Flight Segmentation**

- Each `icao24` track is split into continuous flight segments by detecting gaps > 30 minutes between consecutive position updates
- Each segment receives a unique `flight_id` = `{icao24}_{date}_{seg_id}`
- Segments with fewer than 8 snapshots are discarded

**Airport Proximity UDFs**

Three PySpark UDFs are defined using the Haversine formula and reference airports:

| Airport | ICAO | Location |
|---------|------|----------|
| Madrid Barajas | LEMD | 40.47°N, 3.56°W |
| Barcelona El Prat | LEBL | 41.30°N, 2.08°E |
| Lisbon | LPPT | 38.78°N, 9.14°W |
| Porto | LPPR | 41.25°N, 8.68°W |
| Seville | LEZL | 37.42°N, 5.89°W |
| Valencia | LEVC | 39.49°N, 0.48°W |
| Malaga | LEMG | 36.67°N, 4.50°W |
| Alicante | LEAL | 38.28°N, 0.56°W |
| Bilbao | LEBB | 43.30°N, 2.91°W |
| Palma de Mallorca | LEPA | 39.55°N, 2.74°E |

**Flight Type Classification**

| Type | Rule |
|------|------|
| `departure` | Start near airport, end NOT near airport |
| `arrival` | Start NOT near airport, end near airport |
| `hop` | Both endpoints near an airport — **excluded from clustering** |
| `pass_through` | Neither endpoint near an airport |

**Trajectory Resampling**

Each trajectory is resampled to exactly `N_WAYPOINTS = 15` equally-spaced lat/lon waypoints using linear interpolation, producing a fixed-length 30-dimensional feature vector suitable for ML clustering.

**BisectingKMeans Corridor Clustering**

```python
km = BisectingKMeans(
    featuresCol='traj_vec', predictionCol='cluster',
    k=k, seed=42, maxIter=30, minDivisibleClusterSize=5.0
)
```

| Flight Type | k (clusters) |
|-------------|-------------|
| `pass_through` | 8 |
| `arrival` | 5 |
| `departure` | 5 |

Pipeline: `VectorAssembler → StandardScaler → BisectingKMeans`

Corridor IDs are assigned based on detected departure/arrival airports (e.g. `LEMD-ARR-2`, `PT-5`).

### 6.6 Part III — Corridor Deviation Classifier (Random Forest)

**Train/Test Split — by flight, not by row**

```python
unique_flights = labelled_points.select("flight_id").distinct()
train_flights, test_flights = unique_flights.randomSplit([0.8, 0.2], seed=42)
```

> Splitting by `flight_id` prevents data leakage — all snapshots from a given flight stay in one partition.

**Feature Columns**

| Feature | Description |
|---------|-------------|
| `latitude` | Current position latitude |
| `longitude` | Current position longitude |
| `track_sin` | Sine of true_track (circular encoding) |
| `track_cos` | Cosine of true_track (circular encoding) |
| `vrate_clamped` | Vertical rate clamped to [-30, +30] m/s |

**ML Pipeline**

```
StringIndexer → VectorAssembler → StandardScaler → RandomForestClassifier → IndexToString
```

RandomForest configuration: `numTrees=100`, `maxDepth=8`, `featureSubsetStrategy='sqrt'`

**Deviation Flag**

Each prediction is tagged with a status based on the model's max class probability:

| Status | Condition |
|--------|-----------|
| `on_corridor` | max_probability ≥ 0.70 |
| `marginal` | max_probability ≥ 0.45 |
| `deviating` | max_probability < 0.45 |

---

## 7. Insights & Results

### 7.1 Traffic by Country & Airline

| Country | Unique A/C | Market Share | Avg Speed (km/h) |
|---------|-----------|-------------|-----------------|
| Spain | **141** | **26.4%** | 569 |
| Ireland | 57 | 10.7% | 779 |
| France | 52 | 9.7% | 602 |
| United Kingdom | 39 | 7.3% | 765 |
| Portugal | 38 | 7.1% | 527 |
| Morocco | 17 | 3.2% | **821** |

Top airline: **Ryanair (RYR)** — 83–91 unique aircraft, nearly double its nearest competitor.

### 7.2 Congestion & Density

- Highest density corridor: **NE→SW across central Spain** aligned with the main European Airway Route Network
- Secondary cluster: **Catalonian coast** (Barcelona FIR boundary)
- Atlantic approach lanes to Lisbon/Porto visible along the Portuguese coast (38–42°N)

### 7.3 Corridor Clustering

- BisectingKMeans identified distinct **arrival, departure, and overfly corridors** consistent with known Iberian geography
- **Entry/exit position** and **heading (true_track)** are the most discriminating features for corridor membership
- The Random Forest reliably predicts corridor membership from instantaneous aircraft state

---

## 8. Conclusions

1. Full pipeline validated end-to-end: NiFi → MinIO → Spark → visualisation
2. Three distinct Spark ML capabilities exercised: BisectingKMeans, RandomForest, Spark SQL
3. Spain holds ~25.6% of airspace; Ryanair is the single largest operator
4. Two-tier speed structure confirmed: long-haul transit vs. short-haul domestic
5. Corridor clustering successfully partitions Iberian airspace into operationally meaningful zones
6. The Random Forest deviation classifier provides a foundation for real-time ATC alerting

---

## 9. Future Work

- **24-hour ingestion** to expose full daily traffic cycles and morning vs. evening peak patterns
- **Silver layer** Spark transformations to enrich data with nearest-airport lookup
- **Gold layer** persistence using Apache Cassandra or PostgreSQL for live dashboard serving
- **Weather & NOTAM integration** to reduce false positive deviation alerts from legitimate re-routing
- **Finer 0.25° spatial grid** for better resolution near major airports
- **Apache Superset** dashboard connected to Gold layer for real-time operational monitoring
- **Anomaly detection** using Spark ML Isolation Forest on velocity/altitude deviations

---

*Airspace Intelligence — IE University | Modern Data Architectures for Big Data II | March 2026*
