import pandas as pd
import os

"""
search_movies.py - Movie Search Utilities

Purpose:
Simple string-based search functions for movie discovery.
Uses pipe-delimited columns for partial matching.

Search Types:
1. Genre + Actor + Action filter, ranked by rating
2. Actor + Director co-occurrence, ranked by runtime

Dependencies:
- pandas
- processed_movies.csv

Example:
```python
from search_movies import search_genre
print(search_genre("Science Fiction", "Bruce Willis"))
```
"""

filepath = 'tmdb/processed_movies.csv'


def load_csv(filepath):
    """
    Standard CSV loader (same as kpi.py).
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Unexpected error while loading CSV: {e}")

    return df


df = load_csv(filepath)


def search_genre(genre, cast):
    """
    Find best-rated Action movies in genre starring actor.

    Parameters:
    -----------
    genre : str
        Genre name substring (e.g. 'Science Fiction')
    cast : str
        Actor name substring

    Returns:
    --------
    pd.DataFrame
        Action + genre + cast movies, sorted by vote_average DESC

    Example:
    --------
    >>> search_genre("Science Fiction", "Bruce Willis")
    # Fifth Element, Looper, etc.
    """
    return df[
        df["genres"].str.contains(genre) &
        df["genres"].str.contains("Action") &
        df["cast"].str.contains(cast)
    ].sort_values(by="vote_average", ascending=False)


def search_cast_and_director(cast, director):
    """
    Find shortest movies with actor + director collaboration.

    Parameters:
    -----------
    cast : str
        Actor name substring
    director : str
        Director name substring

    Returns:
    --------
    pd.DataFrame
        Matching movies sorted by runtime ASC

    Example:
    --------
    >>> search_cast_and_director("Uma Thurman", "Quentin Tarantino")
    # Pulp Fiction, Kill Bill Vol.1, etc.
    """
    return df[
        df["cast"].str.contains(cast) &
        df["director"].str.contains(director)
    ].sort_values(by="runtime", ascending=True)


if __name__ == "__main__":
    """
    Demo searches.
    """
    print("Best-rated Science Fiction Action Movie: ",
          search_genre("Science Fiction", "Bruce Willis"))
    print()
    print("Movies starring Uma Thurman & directed by Quentin Tarantino: ",
          search_cast_and_director("Uma Thurman", "Quentin Tarantino"))
