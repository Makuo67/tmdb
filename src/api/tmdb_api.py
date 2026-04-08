import os
import logging
from typing import List, Optional, Dict, Any
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
DEFAULT_MOVIE_IDS: List[int] = [
    299534, 19995, 140607, 299536, 597, 135397, 420818, 24428,
    168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445,
    321612, 260513
]

BASE_URL = "https://api.themoviedb.org/3/movie/"
TIMEOUT = 15
MAX_WORKERS = 10

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# PYDANTIC MODEL
# ----------------------------


class Movie(BaseModel):
    id: int
    title: str
    release_date: Optional[str]
    vote_average: Optional[float]
    credits: Optional[Dict[str, Any]]

    class Config:
        extra = "allow"

# ----------------------------
# SESSION WITH RETRIES
# ----------------------------


def create_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session

# ----------------------------
# FETCH FUNCTION
# ----------------------------


def get_movie(
    movie_id: int,
    api_key: str,
    session: requests.Session
) -> Optional[Dict[str, Any]]:

    def fetch(url, params):
        for attempt in range(5):
            try:
                response = session.get(url, params=params, timeout=TIMEOUT)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.warning(
                    f"[{movie_id}] Attempt {attempt+1}/5 failed: {e}"
                )
        return None

    # Fetch movie + credits in one call
    url = f"{BASE_URL}{movie_id}"
    params = {"api_key": api_key, "append_to_response": "credits"}
    movie = fetch(url, params)

    if movie is None:
        logger.error(f"[{movie_id}] Skipping: could not fetch movie")
        return None

    # Ensure credits exist; retry separately if not included
    if "credits" not in movie or not isinstance(movie["credits"], dict):
        logger.warning(
            f"[{movie_id}] Missing credits. Retrying credits endpoint...")
        credits_url = f"{BASE_URL}{movie_id}/credits"
        credits = fetch(credits_url, {"api_key": api_key})

        if credits is None:
            logger.error(
                f"[{movie_id}] Skipping: credits missing after retries")
            return None

        movie["credits"] = credits

    return movie

# ----------------------------
# VALIDATION
# ----------------------------


def validate_movie(data: Dict[str, Any]) -> Optional[Movie]:
    try:
        return Movie(**data)
    except ValidationError as e:
        logger.warning(f"Validation failed: {e}")
        return None

# ----------------------------
# MAIN LOADER
# ----------------------------


def load_data(
    movie_ids: Optional[List[int]] = None,
    api_key: Optional[str] = None
) -> pd.DataFrame:

    if movie_ids is None:
        movie_ids = DEFAULT_MOVIE_IDS

    api_key = api_key or os.getenv("TMDB_API_KEY")
    if not api_key:
        raise ValueError("TMDB_API_KEY must be set")

    session = create_session()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_movie, movie_id, api_key, session): movie_id
            for movie_id in movie_ids
        }

        for future in as_completed(futures):
            movie_id = futures[future]
            data = future.result()

            if not data:
                continue

            validated = validate_movie(data)
            if validated:
                results.append(validated.dict())

    df = pd.DataFrame(results)

    logger.info(f"Loaded {len(df)} valid movies out of {len(movie_ids)}")
    return df


if __name__ == "__main__":
    print(load_data())
