# Airspace Intelligence — Presentation Script
## 10-Minute Presentation · Big Data Architectures II

---

## SLIDE OVERVIEW

| # | Title | Approx. Time |
|---|-------|-------------|
| 1 | Title slide | 0:30 |
| 2 | Problem & Motivation | 0:45 |
| 3 | System Architecture & Data Pipeline | 1:15 |
| 4 | Data: OpenSky ADS-B & Schema | 0:45 |
| 5 | Part I — Traffic Analysis (Country & Airline) | 1:00 |
| 6 | Part I — Airspace Density Heatmap | 0:45 |
| 7 | Part II — Feature Engineering & Trajectory Resampling | 1:00 |
| 8 | Part II — BKMeans Corridor Clustering (Maps) | 1:15 |
| 9 | Part III — Random Forest Corridor Classifier | 1:00 |
| 10 | Part IV — Real-Time Streaming with Kafka | 0:45 |
| 11 | Summary & Conclusions | 0:45 |

**Total: ~10 minutes**

---

## SLIDE 1 — Title Slide

### What to put on the slide
- Title: **"Airspace Intelligence — Big Data Analysis of the Iberian Peninsula"**
- Subtitle: *Modern Data Architectures for Big Data II — Group Assignment*
- Tech stack logos or text badges: **Apache NiFi · MinIO · Apache Spark · MLlib · Kafka**
- Group member names and date

### What to say
> "Good morning / afternoon. Our project is called Airspace Intelligence. The goal was to build a full end-to-end big data pipeline to monitor, analyse, and predict aircraft behaviour over the Iberian Peninsula — in real time. We used a modern lakehouse stack: NiFi for ingestion, MinIO as object storage, and Spark with MLlib for all the heavy processing and machine learning. Let me walk you through everything we built."

---

## SLIDE 2 — Problem & Motivation

### What to put on the slide
- One-sentence problem statement: *"The Iberian Peninsula handles thousands of flights daily — how do we detect congestion and identify when an aircraft deviates from its expected corridor at scale?"*
- Three bullet points (the three questions we answer):
  1. Who is flying over Iberian airspace and where are the busiest zones?
  2. What are the natural air corridors being used by aircraft?
  3. Can we classify and flag corridor deviations in real time?
- A small illustrative icon or silhouette of the Iberian Peninsula

### What to say
> "Air traffic management is fundamentally a big data problem. Every 10 seconds, hundreds of aircraft broadcast their position, altitude, and speed via ADS-B transponders. The challenge is: how do you make sense of that fire-hose of data? We structured our work around three analytical questions. First, descriptive — who is flying and where is traffic concentrated? Second, unsupervised learning — can we discover the natural corridors aircraft use? And third, supervised learning plus streaming — can we classify any aircraft into its corridor in real time and flag if it's deviating? These three questions drove the entire pipeline design."

---

## SLIDE 3 — System Architecture & Data Pipeline

### What to put on the slide
- A clean left-to-right architecture diagram with these five stages connected by arrows:
  1. **OpenSky Network API** (ADS-B data source, ~10 sec polling)
  2. **Apache NiFi** (ingestion / routing)
  3. **MinIO — Bronze Layer** (raw JSON snapshots, S3-compatible object store)
  4. **Apache Spark / PySpark** (batch processing, SQL, MLlib)
  5. **Apache Kafka** (streaming input for real-time inference)
- Annotate MinIO with: *"airspace-intelligence-bronze bucket"*
- Annotate Spark with: *"SQL · BKMeans · RandomForest · Structured Streaming"*

### What to say
> "Here is the full architecture. On the left, the OpenSky Network API gives us real-time ADS-B state vectors — one snapshot every 10 seconds covering every aircraft over the Iberian Peninsula. NiFi polls this API and lands raw JSON files into our MinIO Bronze bucket — this is our immutable source-of-truth layer. From there, Spark reads everything, applies schema parsing, and runs all the analysis. At the end of the notebook, we introduce Kafka as the streaming transport so we can score live data with the trained model. The design follows a classic lakehouse medallion pattern: ingest raw, process in Spark, serve predictions downstream."

---

## SLIDE 4 — Data: OpenSky ADS-B & Schema

### What to put on the slide
- Table showing the key fields extracted from each ADS-B state vector:

| Field | Type | Description |
|-------|------|-------------|
| `icao24` | String | Unique aircraft transponder ID |
| `callsign` | String | Flight number / operator code |
| `origin_country` | String | Country of registration |
| `latitude / longitude` | Double | Current position |
| `baro_altitude` | Double | Barometric altitude (metres) |
| `velocity` | Double | Ground speed (m/s) |
| `true_track` | Double | Heading (degrees) |
| `vertical_rate` | Double | Climb/descent rate (m/s) |
| `on_ground` | Boolean | Is the aircraft on the ground? |

- A note: *"Filtered to Iberian bounding box: lat 36.4–43.2°, lon −10–2.5°"*

### What to say
> "Each API snapshot is a JSON object containing a `states` array — one inner list per aircraft. We define a strict schema and use Spark's `explode` to unpack each snapshot into individual rows, one per aircraft per 10-second window. After filtering to the Iberian Peninsula bounding box and dropping nulls, we end up with a clean DataFrame — our base for everything that follows. The fields we care most about are position, altitude, velocity, heading and vertical rate — these are the inputs to both the clustering and the classifier."

---

## SLIDE 5 — Part I: Traffic Analysis — Country & Airline

### What to put on the slide
- The three charts from cell 26 (or screenshots of them):
  1. **Horizontal bar chart** — Top 15 countries by unique aircraft
  2. **Horizontal bar chart** — Top 10 airlines by callsign prefix (IBE, VLG, RYR, TAP…)
  3. **Pie chart** — Airspace market share % by country
- Key callout box:
  - 🇪🇸 Spain: ~25.6% market share
  - 🇮🇪 Ireland (Ryanair): 2nd place, 91 unique aircraft
  - Transit traffic from UK, Germany, Switzerland is significant

### What to say
> "The first thing we asked was: who is flying over the Iberian Peninsula? Using Spark SQL, we aggregated distinct ICAO24 transponder addresses by country of registration and by the 3-letter ICAO callsign prefix — which identifies the airline. Spain dominates with around 25% of airspace activity, which makes sense given the large domestic network. But what's interesting is that Ireland ranks second — driven almost entirely by Ryanair, which was the single most active operator with 91 unique aircraft observed. A substantial share also comes from pass-through traffic — UK, German and Swiss-registered aircraft transiting to Africa or Latin America. This confirms the Iberian Peninsula's strategic position on major global airways."

---

## SLIDE 6 — Part I: Airspace Density Heatmap

### What to put on the slide
- A screenshot of the Folium interactive density heatmap (the congestion map)
- Short legend explanation: colour gradient from cool (low density) to hot (high congestion)
- Three callout annotations on the map:
  1. **NE → SW corridor** across central Spain — main European airway to Canaries/Africa
  2. **Catalonian coast** cluster — high traffic near Barcelona El Prat
  3. **Portuguese Atlantic coast** corridor — approaches to Lisbon and Porto

### What to say
> "We then built a spatial density heatmap. We snapped every aircraft observation onto a 1°×1° grid and counted unique aircraft per cell. A log-transform was applied to prevent a few extreme hotspots from washing out the rest of the map. Three zones immediately stand out: a dominant northeast-to-southwest corridor across central Spain, which aligns with the UL upper airway network connecting central Europe to the Canary Islands and North Africa. A secondary cluster over the Catalan coast driven by Barcelona's traffic. And a visible Atlantic approach lane along the Portuguese coast between latitudes 38 and 42 north. These heatmap findings directly motivated the choice of corridor clustering as our next analytical step."

---

## SLIDE 7 — Part II: Feature Engineering & Trajectory Resampling

### What to put on the slide
- Visual pipeline diagram with 4 steps:
  1. **Gap Detection** — split each `icao24` track into flight segments (30-min gap threshold)
  2. **Flight Type Classification** — `departure` / `arrival` / `pass_through` (hop excluded)
  3. **Resampling** — every trajectory interpolated to exactly **15 waypoints** (30 features)
  4. **Feature vector** — `[lat₀, lon₀, lat₁, lon₁, … lat₁₄, lon₁₄]`
- Classification rule table:

| Type | Rule |
|------|------|
| Departure | Start ≤ 10 km from airport, end far |
| Arrival | End ≤ 10 km from airport, start far |
| Pass-through | Neither endpoint near any airport |
| Hop (excluded) | Both endpoints near airports |

### What to say
> "Before we could cluster, we had to convert raw ADS-B observations into something a machine learning algorithm can consume. A single aircraft might have hundreds of snapshots — all of variable length. We did this in three steps. First, we detected flight segment boundaries by looking for gaps larger than 30 minutes in consecutive timestamps for the same transponder. Second, we classified each segment as a departure, arrival, or pass-through using a Haversine-based UDF to check proximity to 10 major Iberian airports. Short intra-peninsular hops were excluded. Third, we resampled every trajectory to exactly 15 equally-spaced waypoints using linear interpolation — giving us a consistent 30-dimensional feature vector per flight. This fixed-length representation is what the clustering algorithm actually operates on."

---

## SLIDE 8 — Part II: BKMeans Corridor Clustering (Maps)

### What to put on the slide
- **Two map screenshots** side by side:
  - Left: **Pass-through corridors map** (8 clusters, colour-coded by corridor ID — PT-0 through PT-7)
  - Right: **Arrival & Departure corridors map** (5 arrival + 5 departure clusters, colour-coded green for arrivals, blue for departures)
- Short methodology bullet:
  - Algorithm: **Bisecting K-Means** (Spark MLlib)
  - K values: 8 pass-through, 5 arrival, 5 departure
  - Evaluation: Silhouette score (Squared Euclidean)
- Why BKMeans over standard KMeans: more balanced clusters, hierarchical structure

### What to say
> "We applied Bisecting K-Means from Spark MLlib separately for each flight type. We chose BKMeans over standard K-Means because it tends to produce more balanced, more deterministic clusters — and it naturally reveals hierarchical corridor structure, like a broad Atlantic corridor that subdivides into Lisbon-bound and Porto-bound sub-corridors. For pass-through flights we used 8 clusters, and 5 each for arrivals and departures. The left map shows the pass-through corridors — you can clearly see distinct colour bands aligning with the major airways we identified in the heatmap. The right map shows arrival and departure corridors labelled by their associated airport — for example ARR-LEMD for flights arriving into Madrid, or DEP-LEBL for departures out of Barcelona. These corridor IDs become the labels for our supervised classifier."

---

## SLIDE 9 — Part III: Random Forest Corridor Classifier

### What to put on the slide
- Model overview box:
  - Algorithm: **Random Forest Classifier** (Spark MLlib)
  - Input features: `latitude, longitude, track_sin, track_cos, vrate_clamped`
  - Target: `corridor_id` (18 classes: PT-0…PT-7, ARR-*, DEP-*)
  - Split: 80% train / 20% test, split **by flight** (not by row) to prevent leakage
- ML Pipeline steps: StringIndexer → VectorAssembler → StandardScaler → RandomForest → IndexToString
- Performance metrics (placeholder for actual values from the notebook output):
  - **F1 Score, Accuracy, Precision, Recall**
- Deviation detection logic:

| Max Class Probability | Status |
|---|---|
| ≥ 0.70 | ✅ on_corridor |
| 0.50 – 0.70 | ⚠️ marginal |
| < 0.50 | 🔴 deviating |

- Final note: **Model saved to MinIO** — `s3a://airspace-intelligence-bronze/models/corridor_classifier/`

### What to say
> "With corridor labels propagated down to every individual ADS-B snapshot, we trained a Random Forest classifier to predict which corridor any aircraft belongs to at a given instant — using only observable real-time features: position, a circular encoding of heading, and vertical rate. A key design decision was splitting train and test by flight ID, not by row — this prevents data leakage where snapshots from the same flight appear on both sides of the split. The model outputs both a class prediction and a probability vector. We use the maximum class probability as a confidence score: above 70% the aircraft is considered on-corridor, between 50 and 70 it's marginal, and below 50 it's flagged as potentially deviating. This transforms a simple classifier into an early-warning deviation detector. The trained model pipeline is then persisted to MinIO so it can be reloaded for streaming inference."

---

## SLIDE 10 — Part IV: Real-Time Streaming with Kafka

### What to put on the slide
- A streaming pipeline diagram:
  **OpenSky API → Kafka Topic (`opensky_raw`) → Spark Structured Streaming → ML Model → Output**
- Four code-level steps with short labels:
  1. **New SparkSession** configured with Kafka + MinIO packages
  2. **Load model** from MinIO (`PipelineModel.load(MODEL_PATH)`)
  3. **readStream** from Kafka topic `opensky_raw`, `startingOffsets = latest`
  4. **Transform & predict**: parse JSON → explode → feature engineering → model inference → status flag
- Output columns shown: `snapshot_time, icao24, callsign, latitude, longitude, predicted_corridor, max_probability, status`
- Processing trigger: every 10 seconds

### What to say
> "The final piece of the system brings everything together in real time. We start a new Spark session — this time configured with the Spark-Kafka connector package — and reload the trained Random Forest pipeline from MinIO. We then open a Structured Streaming reader on the Kafka topic `opensky_raw`, which receives the same live OpenSky API feed that NiFi is writing. The stream processing mirrors the batch pipeline: parse the JSON payload, explode the states array, apply the same feature transformations, and then run the loaded model to produce predictions. Every 10 seconds a new micro-batch is scored, and each aircraft gets a `predicted_corridor` label and a `status` — on corridor, marginal, or deviating — in near real time. This is the operational endpoint of the entire system."

---

## SLIDE 11 — Summary & Conclusions

### What to put on the slide
- End-to-end pipeline summary table:

| Stage | Technology | Output |
|-------|-----------|--------|
| Data Collection | NiFi + OpenSky API | Raw JSON in MinIO Bronze |
| Batch Processing | PySpark SQL + DataFrames | Cleaned `aircraft_df` |
| Descriptive Analysis | Spark SQL + Folium | Traffic charts + Density heatmap |
| Trajectory Engineering | PySpark Window + UDFs | Fixed 30-dim flight vectors |
| Corridor Discovery | BisectingKMeans (MLlib) | 18 labelled corridors |
| Corridor Classification | RandomForest (MLlib) | Trained classifier + deviation flag |
| Real-Time Inference | Kafka + Spark Streaming | Live status per aircraft |

- Key takeaways (3 bullets):
  1. **Spain dominates** Iberian airspace (~25.6%), but transit traffic from non-Iberian carriers is substantial
  2. **BKMeans** successfully discovered geographically coherent corridors aligned with known airways
  3. **Random Forest** enables real-time corridor classification and deviation detection at streaming scale

### What to say
> "To wrap up — we built a full big data pipeline that goes from raw ADS-B signals all the way to real-time deviation alerts. The descriptive analysis confirmed Spain's dominance and revealed the three main density corridors. BKMeans discovered 18 meaningful flight corridors — 8 pass-through, 5 arrival, and 5 departure — that align with actual airway structure. The Random Forest classifier converts those corridor labels into a real-time scoring engine capable of flagging deviating aircraft from a live Kafka stream. Everything runs on an open-source stack — NiFi, MinIO, Spark, Kafka — with no proprietary components. Thank you. We are happy to take any questions."

---

*End of presentation script — 11 slides, ~10 minutes.*
