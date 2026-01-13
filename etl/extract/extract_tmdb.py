import os
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime 

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")  

MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513
]

TIMEOUT = 10
RETRY_TOTAL = 3
RETRY_BACKOFF = 1.5
RATE_LIMIT_SLEEP = 0.25
LOG_DIR ="../../log"
os.makedirs(LOG_DIR,exist_ok=True)

log_file = os.path.join(
    LOG_DIR,f"tmdb_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)







logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


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


def fetch_movie(movie_id: int) -> dict | None:
    if movie_id == 0:
        return None

    url = f"{BASE_URL}{movie_id}?api_key={API_KEY}"
    data = get_json(url)

    if not data or "id" not in data:
        logging.warning("Invalid movie payload | movie_id=%s", movie_id)
        return None

    return data


def fetch_credits(movie_id: int) -> dict | None:
    url = f"{BASE_URL}{movie_id}/credits?api_key={API_KEY}"
    data = get_json(url)

    if not data or "cast" not in data or "crew" not in data:
        logging.warning("Invalid credits payload | movie_id=%s", movie_id)
        return None

    return data



raw_records = []

for movie_id in MOVIE_IDS:
    logging.info("Extracting movie_id=%s", movie_id)

    movie_payload = fetch_movie(movie_id)
    if not movie_payload:
        continue

    credits_payload = fetch_credits(movie_id)

    raw_records.append({
        "movie_id": movie_id,
        "movie": movie_payload,        # full raw movie JSON
        "credits": credits_payload     # full raw credits JSON
    })

    time.sleep(RATE_LIMIT_SLEEP)



df_raw = pd.DataFrame(raw_records)

output_path = "../../data/raw/tmdb_movies_raw.csv"
df_raw.to_csv(output_path, index=False)

logging.info(
    "Raw extraction completed | records=%s | path=%s",
    len(df_raw),
    output_path
)
