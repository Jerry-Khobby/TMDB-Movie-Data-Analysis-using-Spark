# main.py

import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession

# ETL
from etl.extract_tmdb import extract_tmdb_movies
from etl.transform import run_transformation

# KPIs
from kpis.kpis_ranking import compute_tmdb_kpis
from kpis.advanced import advanced_tmdb_kpis
from kpis.save_kpi_results import save_kpi_results

# Visualization
from visualisation import visualize_tmdb_from_url





# Paths & Constants
BASE_PATH = "/tmdbmovies/app"
DATA_PATH = f"{BASE_PATH}/data"
LOG_DIR = f"{BASE_PATH}/logs"

RAW_JSON = f"{DATA_PATH}/raw/tmdb_movies_raw.json"
CLEAN_CSV = f"{DATA_PATH}/clean/tmdb_movies_clean.csv"
KPI_OUTPUT_DIR = f"{DATA_PATH}/kpis"

REQUIRED_DIRS = [
    f"{DATA_PATH}/raw",
    f"{DATA_PATH}/clean",
    f"{DATA_PATH}/diagrams",
    KPI_OUTPUT_DIR,
    LOG_DIR,
]



# Setup
def setup_directories():
    for directory in REQUIRED_DIRS:
        os.makedirs(directory, exist_ok=True)


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"tmdb_main_{datetime.now():%Y%m%d_%H%M%S}.log")

    # Define handlers
    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized | log_file={log_file}")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TMDB_End_to_End_Pipeline")
        .getOrCreate()
    )




# Main Pipeline
def main():
    setup_directories()
    setup_logging()

    logger = logging.getLogger(__name__)
    logger.info("Starting TMDB Spark Pipeline")

    spark = create_spark_session()

    try:
        # Extract
        logger.info("Extraction step started")
        extract_tmdb_movies(spark, RAW_JSON)

        # Transform
        logger.info("Transformation step started")
        run_transformation(spark, RAW_JSON, CLEAN_CSV)

        # KPI Analysis
        logger.info("KPI computation started")
        df_clean = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(CLEAN_CSV)
        )

        basic_kpis = compute_tmdb_kpis(df_clean, top_n=10)
        advanced_kpis = advanced_tmdb_kpis(df_clean, top_n=10)
        
        save_kpi_results(
        kpi_results=basic_kpis,
        base_output_dir=KPI_OUTPUT_DIR,
        kpi_group="basic"
        )
        save_kpi_results(
        kpi_results=advanced_kpis,
        base_output_dir=KPI_OUTPUT_DIR,
        kpi_group="advanced"
        )
        logger.info("Computed %d basic KPIs", len(basic_kpis))
        logger.info("Computed %d advanced KPIs", len(advanced_kpis))

        # Visualization
        logger.info("Visualization started")
        visualize_tmdb_from_url(CLEAN_CSV)

        logger.info("TMDB Pipeline completed successfully")

    except Exception:
        logger.exception("TMDB Pipeline failed")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
