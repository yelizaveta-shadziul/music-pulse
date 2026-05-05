# 🎵 Music Pulse — Real-Time Music Analytics

> A real-time music streaming analytics system built with Apache Kafka, Spark, and Streamlit

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-black?logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange?logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-red?logo=streamlit)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

## 📖 About

**Music Pulse** is a data engineering project that demonstrates real-time streaming analytics. The system collects data about trending tracks from the Last.fm API, streams it through Apache Kafka, processes it with Spark Structured Streaming, and visualizes live metrics on a Streamlit dashboard.

### What the dashboard shows

- 🎵 **Average BPM** — current average tempo of trending tracks
- ⚡ **Energy** — average energy level of tracks
- 😊 **Mood Index** — mood score based on valence
- 📈 **BPM over time** — live chart of tempo changes
- 🌡️ **Mood indicator** — visual mood progress bar

---

## 🏗️ Architecture

```
Last.fm API → Kafka Producer → Apache Kafka → Spark Streaming → PostgreSQL → Streamlit
```

| Component | Technology | Description |
|-----------|-----------|-------------|
| Data source | Last.fm API + pylast | Real-time top tracks |
| Message broker | Apache Kafka 7.5.0 | Topic `spotify_stream` |
| Processing | Apache Spark 3.5.1 | Sliding window aggregation |
| Storage | PostgreSQL 16 | Table `music_metrics` |
| Visualization | Streamlit | Updates every 10 seconds |
| Infrastructure | Docker Compose | Kafka, Spark, PostgreSQL in containers |

---

## 📁 Project structure

```
music-pulse/
├── docker-compose.yml        # Infrastructure (Kafka, Spark, PostgreSQL)
├── .env                      # Secret keys (not uploaded to GitHub)
├── .env.example              # Example env file
├── .gitignore                # Files to ignore
├── producer/
│   ├── producer.py           # Kafka Producer — fetches data from Last.fm
│   └── requirements.txt      # Producer dependencies
├── spark/
│   └── spark_job.py          # Spark Streaming Job — processes data
└── dashboard/
    ├── app.py                # Streamlit dashboard
    └── requirements.txt      # Dashboard dependencies
```

---

## 🚀 Getting started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) (at least 4 GB RAM)
- [Python 3.11+](https://www.python.org/downloads/)
- [Java 17](https://adoptium.net/)
- A [Last.fm API key](https://www.last.fm/api/account/create)

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/music-pulse.git
cd music-pulse
```

### 2. Set up environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your keys:

```env
LASTFM_API_KEY=your_lastfm_api_key_here
```

### 3. Start the infrastructure

```bash
docker compose up -d
```

### 4. Create the database table

```bash
docker exec -it music-pulse-postgres-1 psql -U music -d musicpulse -c "CREATE TABLE IF NOT EXISTS music_metrics (id SERIAL PRIMARY KEY, window_start TIMESTAMP, window_end TIMESTAMP, avg_tempo FLOAT, avg_energy FLOAT, avg_valence FLOAT, created_at TIMESTAMP DEFAULT NOW());"
```

### 5. Install dependencies

```bash
pip install -r producer/requirements.txt
pip install streamlit psycopg2-binary pandas
```

### 6. Run all components

Open **3 separate terminals**:

**Terminal 1 — Producer:**
```bash
python producer/producer.py
```

**Terminal 2 — Spark (PowerShell):**
```powershell
docker cp spark/spark_job.py music-pulse-spark-master-1:/opt/spark/work-dir/spark_job.py
docker exec -it music-pulse-spark-master-1 /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1 /opt/spark/work-dir/spark_job.py
```

**Terminal 3 — Dashboard:**
```bash
streamlit run dashboard/app.py
```

### 7. Open the dashboard

Go to [http://localhost:8501](http://localhost:8501) in your browser.
Wait 2-3 minutes for Spark to accumulate the first data points.

---

## 🐛 Common issues

| Problem | Solution |
|---------|----------|
| `NoBrokersAvailable` | Make sure Docker is running: `docker compose ps` |
| `UndefinedTable` | Recreate the table (step 4) |
| Dashboard not updating | Wait 2-3 minutes for Spark to collect data |
| Docker freezing | Need at least 4 GB of free RAM |

---

## 📊 Sample message

```json
{
  "track_id": "-1234567890",
  "track_name": "Creep",
  "artist": "Radiohead",
  "energy": 0.76,
  "tempo": 92.5,
  "valence": 0.34,
  "timestamp": "2026-05-05T20:30:00Z"
}
```

---

## 🛠️ Tech stack

- **[Apache Kafka](https://kafka.apache.org/)** — distributed message broker
- **[Apache Spark](https://spark.apache.org/)** — streaming data processing engine
- **[PostgreSQL](https://www.postgresql.org/)** — relational database
- **[Streamlit](https://streamlit.io/)** — Python dashboard framework
- **[pylast](https://github.com/pylast/pylast)** — Python client for Last.fm API
- **[Docker Compose](https://docs.docker.com/compose/)** — container orchestration
