import os
import requests
import pandas as pd
from typing import List, Optional, Dict, Any


"""
tmdb_api.py - TMDB API Data Extraction Layer

Purpose:
High-performance API client for TMDB movie data with credits.
Fetches raw JSON for multiple movies in batch.

Dependencies:
- requests
- pandas
- TMDB_API_KEY environment variable (.env)

Data Contract:
- Input: List of TMDB movie IDs (default: 18 blockbuster movies)
- Output: pandas.DataFrame with nested JSON (belongs_to_collection, credits, etc.)

Example:
```python
from tmdb_api import load_data
df_raw = load_data([299534])  # Avengers: Endgame
```
"""


DEFAULT_MOVIE_IDS: List[int] = [299534, 19995, 140607, 299536, 597, 135397, 420818, 24428,
                                168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]
API_KEY: Optional[str] = os.getenv('TMDB_API_KEY')
BASE_URL = 'https://api.themoviedb.org/3/movie/'


def get_movie(movie_id: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch complete movie details from TMDB API including credits.

    Parameters:
    -----------
    movie_id : int
        TMDB movie ID (e.g., 299534 for Avengers: Endgame)
    api_key : str
        TMDB API key from environment or parameter

    Returns:
    --------
    Dict[str, Any] or None
        Raw movie JSON with credits appended, None on API error

    Raises:
    -------
    requests.RequestException
        Network/HTTP errors
    ValueError
        Missing API key

    Example:
    --------
    >>> data = get_movie(299534, 'your_api_key')
    >>> data['title']  # 'Avengers: Endgame'
    """
    if not api_key:
        raise ValueError("TMDB_API_KEY environment variable is required.")
    url = f"{BASE_URL}{movie_id}?api_key={api_key}&append_to_response=credits"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def load_data(movie_ids: Optional[List[int]] = None, api_key: Optional[str] = None) -> pd.DataFrame:
    """
    Batch load movie data from TMDB API into cleaned DataFrame.

    Parameters:
    -----------
    movie_ids : List[int], optional
        TMDB movie IDs to fetch (default: blockbuster movies)
    api_key : str, optional
        TMDB API key (falls back to TMDB_API_KEY env var)

    Returns:
    --------
    pd.DataFrame
        Raw movie data with columns: id, title, budget, revenue, credits (nested JSON), etc.

    Notes:
    ------
    - Graceful handling of API failures (continues batch)
    - Default IDs yield ~20 high-profile movies for analysis
    - Essential input for pre_process.py ETL pipeline

    Example:
    --------
    >>> df = load_data()
    >>> df.shape  # (18, 25+) depending on successful fetches
    >>> df['title'].tolist()  # ['Avengers: Endgame', 'Avatar', ...]
    """
    if movie_ids is None:
        movie_ids = DEFAULT_MOVIE_IDS
    if api_key is None:
        api_key = os.getenv("TMDB_API_KEY")

    if not api_key:
        raise ValueError("TMDB_API_KEY environment variable must be set.")

    movies_data = []
    for movie_id in movie_ids:
        try:
            data = get_movie(movie_id, api_key)
            if data:  # Skip None
                movies_data.append(data)
        except requests.RequestException:
            print(f"Failed to fetch movie {movie_id}")
            continue

    return pd.DataFrame(movies_data)
