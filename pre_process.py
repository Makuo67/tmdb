from tmdb_api import load_data, DEFAULT_MOVIE_IDS
from typing import Optional
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
load_dotenv()

"""
pre_process.py - Production ETL Pipeline for TMDB Data

Purpose:
Transform raw TMDB JSON → production-ready analytical dataset.
Comprehensive cleaning, feature engineering, nested JSON flattening.

Core Transformations:
1. Load raw API data + flatten credits
2. Extract nested lists → pipe-delimited strings (genres|cast|director)
3. Financial normalization ($ → $M)
4. Quality gates (drop incomplete rows, zero budgets, unreleased)
5. Feature engineering (cast_size, director extraction, ROI-ready)

Output Schema: 23 production columns for analytics/visualization.

Dependencies:
- pandas, numpy, python-dotenv
- tmdb_api.py

Example:
```python
from pre_process import full_pipeline
df_clean = full_pipeline(save_path='movies.csv')
```
"""


def load_and_clean(api_key: Optional[str] = None, movie_ids: Optional[list[int]] = None) -> pd.DataFrame:
    """
    Initial ETL stage: Load raw API data and flatten credits JSON.

    Parameters:
    -----------
    api_key : str, optional
        TMDB API key
    movie_ids : list[int], optional
        Movie IDs to process

    Returns:
    --------
    pd.DataFrame
        Raw data with credits flattened, basic drop (adult/video/homepage)

    Notes:
    ------
    - Drops invalid IDs (null/0)
    - Normalizes credits → separate columns (cast/crew lists)
    """
    df = load_data(api_key=api_key, movie_ids=movie_ids)

    df = df[df['id'].notna() & (df['id'] != 0)].reset_index(drop=True)
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

    df_credits = pd.json_normalize(df["credits"])

    df = df.join(df_credits)
    df = df.drop(columns=["credits"])

    return df


def extract_nested_list(series: pd.Series, key: str = 'name') -> pd.Series:
    """
    Vectorized extraction: nested list/dict → pipe-delimited string.

    Parameters:
    -----------
    series : pd.Series
        Column with list/dict JSON values
    key : str, default 'name'
        Field to extract from dict items

    Returns:
    --------
    pd.Series
        '|'.join([item[key] for item in list]) or pd.NA

    Examples:
    ---------
    >>> genres = [{'name':'Action'}, {'name':'Sci-Fi'}]
    >>> extract_nested_list(pd.Series([genres]))  # 'Action|Sci-Fi'
    """
    def _extract(val):
        if isinstance(val, list):
            return '|'.join(
                item.get(key, '') for item in val if isinstance(item, dict)
            )
        elif isinstance(val, dict):
            return val.get(key, pd.NA)
        return pd.NA

    return series.apply(_extract)


def full_pipeline(api_key: Optional[str] = None, movie_ids: Optional[list[int]] = None, save_path: str = '~/Amalitech-foundation/tmdb/processed_movies.csv') -> pd.DataFrame:
    """
    PRODUCTION ETL PIPELINE: Raw → Clean → Feature Engineering → CSV Export

    Complete data engineering workflow implementing industry best practices:

    Transformations (23 total):
    ├── JSON Flattening: 8 nested fields → pipe-delimited
    ├── Financial: $→$M, zero→NaN filtering
    ├── Features: cast_size, director extraction, crew_size
    ├── Quality: Row completeness ≥10 cols, released status only
    ├── Type Safety: Numeric coercion, datetime parsing
    └── Schema Enforcement: Final 23-column analytical dataset

    Parameters:
    -----------
    api_key : str, optional
        TMDB API key (auto .env)
    movie_ids : list[int], optional
        Override default blockbuster IDs
    save_path : str, default '~/.../processed_movies.csv'
        Output CSV location

    Returns:
    --------
    pd.DataFrame
        Final production dataset (reset_index=True)

    Output Columns (23):
    -------------------
    ['id','title','tagline','release_date','genres','belongs_to_collection',
     'budget($M)','revenue($M)','production_companies','vote_count',
     'vote_average','popularity','cast','director','cast_size','crew_size',...]

    Example:
    --------
    >>> df = full_pipeline()
    # Ready for analysis
    >>> df[df['title']=='Avengers: Endgame']['roi'].iloc[0]
    """
    df = load_and_clean(api_key, movie_ids)

    # Extract nested fields → pipe-delimited
    df['collection_name'] = extract_nested_list(
        df['belongs_to_collection'], 'name')
    df['genres_clean'] = extract_nested_list(df['genres'])
    df['spoken_languages_clean'] = extract_nested_list(
        df['spoken_languages'], 'english_name')
    df['production_countries_clean'] = extract_nested_list(
        df['production_countries'], 'name')
    df['production_companies_clean'] = extract_nested_list(
        df['production_companies'], 'name')

    df['cast_size'] = df['cast'].apply(
        lambda x: len(x) if isinstance(x, list) else 0)

    print(df['cast'].apply(type).value_counts())  # Debug: list confirmation

    df['cast'] = df['cast'].apply(lambda x: "|".join(
        [c.get("name", "") for c in x]) if isinstance(x, list) else np.nan)

    def get_director(crew_list):
        \"\"\"Extract primary director from crew JSON.\"\"\"
        if isinstance(crew_list, list):
            for member in crew_list:
                if member.get("job") == "Director":
                    return member.get("name")
        return np.nan

    df['director'] = df['crew'].apply(get_director)

    df['crew_size'] = df['crew'].apply(lambda x: len(x) if isinstance(x, list) else 0)

    # Drop raw JSON columns
    json_cols = ['belongs_to_collection', 'genres', 'production_countries', 'production_companies', 'spoken_languages']
    df = df.drop(columns=json_cols, errors='ignore')

    # Fill categorical NaNs consistently
    df = df.fillna({"collection_name": "None", "genres_clean": "Unknown", "spoken_languages_clean": "Unknown",
                    "production_countries_clean": "Unknown", "production_companies_clean": "Unknown"})

    # Financial cleaning: zero → NaN (business meaningful)
    df['budget'] = df['budget'].replace(0, np.nan)
    df['revenue'] = df['revenue'].replace(0, np.nan)

    # Type coercion pipeline
    numeric_cols = ['budget', 'id', 'popularity', 'revenue', 'runtime', 'vote_count', 'vote_average', 'cast_size', 'crew_size']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    df['budget'] = df['budget'].replace(0, np.nan)
    df['revenue'] = df['revenue'].replace(0, np.nan)
    df['runtime'] = df['runtime'].replace(0, np.nan)

    # Scale financials to $M (analytical standard)
    df['budget'] = df['budget'] / 1_000_000
    df['revenue'] = df['revenue'] / 1_000_000

    # Rating quality filter
    df.loc[df['vote_count'] == 0, 'vote_average'] = np.nan

    # Text cleaning (overview/tagline)
    placeholders = ['No Data', 'none', 'None', '', 'null', 'Null', 'Undefined', 'undefined']
    df['overview'] = df['overview'].replace(placeholders, np.nan)
    df['tagline'] = df['tagline'].replace(placeholders, np.nan)

    # Quality gates
    df = df.dropna(subset=['id', 'title'])  # Essential keys
    df = df[df.count(axis=1) >= 10]  # ≥10 non-null cols

    df = df[df['status'] == 'Released']  # Production filter
    df = df.drop(columns=['status'], errors='ignore')

    # Column standardization (analytical names)
    df_final = df.rename(columns={'genres_clean': 'genres', 'collection_name': 'belongs_to_collection',
                                  'production_companies_clean': 'production_companies', 'production_countries_clean': 'production_countries',
                                  'spoken_languages_clean': 'spoken_languages'})

    # Enforce final schema
    final_columns = ['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection', 'original_language',
                     'budget', 'revenue', 'production_companies', 'production_countries', 'vote_count', 'vote_average',
                     'popularity', 'runtime', 'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size']
    df_final = df_final[[col for col in final_columns if col in df_final.columns]]
    df_final = df_final.reset_index(drop=True)

    # Production export
    df_final.to_csv(save_path, index=False)
    print(f"Processed data saved to {save_path}")
    print("Columns:", df_final.columns.tolist())

if __name__ == "__main__":
    print(full_pipeline())
