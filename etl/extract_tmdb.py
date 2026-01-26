import os
import time
import logging
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from pyspark.sql import SparkSession
import shutil
import glob

# Import your schema
from etl.data_schema import API_MOVIES_SCHEMA

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")  

# Movie IDs to fetch
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513
]

# Constants
TIMEOUT = 10
RETRY_TOTAL = 3
RETRY_BACKOFF = 1.5
RATE_LIMIT_SLEEP = 0.25
LOG_DIR = "/tmdbmovies/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(
    LOG_DIR, f"tmdb_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Start Spark session

# Create a requests session with retries
def create_session() -> requests.Session:
    retry_strategy = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = create_session()

# Simple GET request handler
def get_json(url: str) -> dict | None:
    try:
        response = session.get(url, timeout=TIMEOUT)
        if response.status_code != 200:
            logging.error(
                "Non-200 response | status=%s | url=%s",
                response.status_code,
                url
            )
            return None
        return response.json()
    except requests.exceptions.RequestException as exc:
        logging.error("HTTP request failed | url=%s | error=%s", url, exc)
        return None

# Fetch movie with credits
def fetch_movie_with_credits(movie_id: int) -> dict | None:
    if movie_id == 0:
        return None
    url = f"{BASE_URL}{movie_id}?api_key={API_KEY}&append_to_response=credits"
    movie_data = get_json(url)
    if not movie_data or "id" not in movie_data:
        logging.warning("Invalid movie payload | movie_id=%s", movie_id)
        return None
    return movie_data

# etl/extract_tmdb.py

def extract_tmdb_movies(
    spark: SparkSession,
    output_path: str = "/tmdbmovies/app/data/raw/tmdb_movies_raw.json"
) -> str:
    records = []

    for movie_id in MOVIE_IDS:
        logging.info("Extracting movie_id=%s", movie_id)
        movie_payload = fetch_movie_with_credits(movie_id)
        if movie_payload:
            records.append(movie_payload)
        time.sleep(RATE_LIMIT_SLEEP)

    df_raw = spark.createDataFrame(records, schema=API_MOVIES_SCHEMA)

    temp_path = output_path + "_tmp"
    df_raw.coalesce(1).write.mode("overwrite").json(temp_path)

    part_file = glob.glob(os.path.join(temp_path, "part-*.json"))[0]
    shutil.move(part_file, output_path)
    shutil.rmtree(temp_path)

    logging.info("Extraction completed | records=%s", df_raw.count())
    return output_path

