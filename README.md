
# TMDB Movie Data Analysis using Apache Spark

## Project Overview

This project implements an **end-to-end movie data analytics pipeline** using **Apache Spark**, Python, and data visualization libraries.
The pipeline extracts movie data from the **TMDb API**, performs structured **ETL processing**, computes **advanced KPIs**, and generates **business-ready visual insights**.

Unlike a purely Pandas-based workflow, this project is designed with **scalability and production-style data engineering practices**, leveraging Spark DataFrames, schemas, modular ETL layers, logging, and orchestration.

---

## Project Objectives

* Extract real-world movie data from the TMDb API
* Apply schema-driven ETL using Apache Spark
* Clean, normalize, and enrich semi-structured JSON data
* Compute advanced financial, popularity, and performance KPIs
* Perform franchise and director-level analysis
* Visualize insights using Pandas + Matplotlib/Seaborn
* Orchestrate the entire pipeline via a single entry point (`main.py`)

---

##  Project Structure

```text
TMDB-Movies-data-analysis-using-spark/
│
├── data/
│   ├── raw/                # Raw JSON data fetched from TMDb API
│   ├── clean/              # Cleaned & transformed Spark output
│   └── diagrams/           # Saved visualization images
│
├── etl/
│   ├── data_schema.py      # Spark schema definitions
│   ├── extract_tmdb.py     # API extraction logic (raw JSON output)
│   └── transform.py        # Data cleaning & transformation logic
│
├── kpis/
│   ├── kpis_ranking.py     # KPI ranking & filtering logic
│   └── advanced.py         # Advanced analytics & business KPIs
│
├── logs/                   # Execution logs (ETL, KPIs, visualization)
│
├── visualisation.py        # Pandas-based data visualizations
├── main.py                 # Pipeline orchestration entry point
├── requirements.txt        # Python dependencies
├── Dockerfile              # Spark application container
├── docker-compose.yml      # Service orchestration
└── README.md
```

---

##  End-to-End Pipeline Flow

###  Data Extraction (ETL – Extract)

* Movie data is fetched from the **TMDb API** using predefined movie IDs.
* A **Spark schema** defined in `data_schema.py` is reused during ingestion.
* Raw API responses are stored as **JSON files** in:

```
data/raw/
```

---

###  Data Cleaning & Transformation (ETL – Transform)

The `transform.py` module performs structured Spark transformations:

#### Key Cleaning Steps

* Drop irrelevant columns:

  ```text
  adult, imdb_id, original_title, video, homepage
  ```
* Normalize nested / JSON-like fields:

  * `genres`
  * `production_companies`
  * `production_countries`
  * `spoken_languages`
  * `belongs_to_collection`
* Convert data types:

  * Budget & revenue → numeric (Million USD)
  * Release date → datetime
* Handle missing & invalid values:

  * Replace 0 budgets/revenues with nulls
  * Filter movies with insufficient metadata
* Remove duplicates
* Keep only **Released** movies
* Reorder and finalize schema

Final cleaned data is stored in:

```
data/clean/
```

---

###  KPI Implementation & Advanced Analysis

All KPI logic is implemented using **Spark DataFrames** for scalability.

#### KPI Rankings

Movies are ranked based on:

* Highest Revenue
* Highest Budget
* Highest Profit
* Lowest Profit
* Highest ROI (Budget ≥ 10M)
* Lowest ROI (Budget ≥ 10M)
* Most Voted
* Highest Rated (≥ 10 votes)
* Lowest Rated (≥ 10 votes)
* Most Popular

Reusable ranking logic is implemented to avoid duplication.

---

####  Advanced Search Queries

* **Search 1**
  Best-rated *Science Fiction & Action* movies starring **Bruce Willis**

* **Search 2**
  Movies starring **Uma Thurman**, directed by **Quentin Tarantino**, sorted by runtime

---

####  Franchise vs Standalone Analysis

Comparison based on:

* Mean Revenue
* Median ROI
* Mean Budget
* Mean Popularity
* Mean Rating

---

#### Success Metrics

* **Most Successful Franchises**

  * Total movies
  * Total & mean revenue
  * Total & mean budget
  * Mean rating

* **Most Successful Directors**

  * Total movies
  * Total revenue
  * Mean rating

---

### Data Visualization & Insights

Visualizations are generated in `visualisation.py` using **Pandas + Matplotlib + Seaborn** after converting Spark DataFrames to Pandas.

Generated plots include:

* Revenue vs Budget trends
* ROI distribution by genre
* Popularity vs rating comparisons
* Yearly box office revenue trends
* Franchise vs standalone revenue comparison

All plots are saved to:

```
data/diagrams/
```

Execution steps and plot generation are logged.

---

## Running the Project

### Using Docker & Spark

Build and start the Spark application:

```bash
docker-compose build
docker-compose up -d
```

Run the pipeline:

```bash
docker exec -it tmdb_movies_spark spark-submit /tmdbmovies/app/main.py
```

---

## Logging

* Each major pipeline stage logs execution details
* Logs include timestamps, row counts, and progress checkpoints
* Logs are written to:

```
logs/
```

---

## Technologies Used

* **Apache Spark (PySpark)**
* **Python**
* **Pandas**
* **Matplotlib & Seaborn**
* **Docker & Docker Compose**
* **TMDb API**

---

