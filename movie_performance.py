
import pandas as pd
import os

"""
movie_performance.py - Advanced Franchise & Director Performance Analytics

Purpose:
Aggregate analysis of franchise success vs standalone, director track records.
Multi-metric ranking (revenue, ratings, movie count) with groupby aggregations.

Key Analyses:
- Franchise vs Standalone: mean revenue/ROI/budget/popularity/rating
- Franchise ranking: total/mean metrics by collection
- Director ranking: explode + aggregate (movies/revenue/rating)

Dependencies:
- pandas
- processed_movies.csv

Example:
```python
from movie_performance import franchise_success, director_success
print(franchise_success().head())
```
"""

filepath = 'data/processed_movies.csv'


def load_csv(filepath):
    """
    Standard CSV loader identical to kpi.py.

    Parameters:
    -----------
    filepath : str
        Path to processed CSV

    Returns:
    --------
    pd.DataFrame
        Loaded dataset
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Unexpected error while loading CSV: {e}")

    return df


df = load_csv(filepath)


def add_movie_type():
    """
    Add movie_type column (Franchise vs Standalone).

    Note: Contains bug - needs df['movie_type'] = assignment.
    """
    df["movie_type"] = df["belongs_to_collection"].apply(
        lambda x: "Franchise" if x is not pd.NA else "Standalone"
    )
    return df


def compare_franchise_vs_standalone():
    """
    Franchise vs Standalone performance comparison.

    Metrics:
    - mean_revenue, median_roi, mean_budget, mean_popularity, mean_rating

    Returns:
    --------
    pd.DataFrame
        2-row summary (Franchise/Standalone)

    Key Insight:
    ------------
    Franchises typically outperform standalone on revenue/ROI.
    """
    df = add_movie_type()
    df["roi"] = df['revenue'] / df['budget']

    grouped = df.groupby("movie_type").agg({
        "revenue": "mean",
        "roi": "median",
        "budget": "mean",
        "popularity": "mean",
        "vote_average": "mean"
    })

    grouped = grouped.rename(columns={
        "revenue": "mean_revenue",
        "roi": "median_roi",
        "budget": "mean_budget",
        "popularity": "mean_popularity",
        "vote_average": "mean_rating"
    })

    return grouped


print("=============== Franchise vs Standalone ==================")
print(compare_franchise_vs_standalone())
print()


def franchise_success():
    """
    Complete franchise performance leaderboard.

    Aggregates:
    - movie_count, total_budget, mean_budget
    - total_revenue, mean_revenue, mean_rating

    Returns:
    --------
    pd.DataFrame
        Franchises ranked by total_revenue

    Key Insight:
    ------------
    Reveals franchise dominance (Avengers, Star Wars typically lead).
    """
    franchises = df[df["belongs_to_collection"].notna()]

    results = franchises.groupby("belongs_to_collection").agg({
        "id": "count",
        "budget": ["sum", "mean"],
        "revenue": ["sum", "mean"],
        "vote_average": "mean"
    })

    results.columns = [
        "movie_count",
        "total_budget",
        "mean_budget",
        "total_revenue",
        "mean_revenue",
        "mean_rating"
    ]

    return results.sort_values(by="total_revenue", ascending=False)


print("=============== Most Successful Movie Franchises ==================")
print(franchise_success())
print()


def top_franchise_by_metric(metric):
    """
    Flexible franchise ranking by any metric.

    Parameters:
    -----------
    metric : str or list[str]
        Column(s) to rank by (auto-converts str→list)

    Returns:
    --------
    pd.DataFrame
        Top 5 franchises by specified metric(s)

    Example:
    --------
    >>> top_franchise_by_metric('movie_count')
    >>> top_franchise_by_metric(['total_revenue', 'mean_revenue'])
    """
    data = franchise_success()

    if isinstance(metric, str):
        metric = [metric]

    return data.sort_values(by=metric, ascending=False).head(5)


print("Total number of movies in franchise")
print(top_franchise_by_metric('movie_count'))
print()
print("Top Franchise by Budget: ")
print(top_franchise_by_metric(["total_budget", "mean_budget"]))
print()
print("Top Franchise by revenue")
print(top_franchise_by_metric(["total_revenue", "mean_revenue"]))
print()
print("Top Franchise by Mean Rating")
print(top_franchise_by_metric('mean_rating'))
print()


def explode_directors():
    """
    Normalize director column for groupby analysis.

    Transformations:
    - Fill NaN → ""
    - Split pipe-delimited → list
    - explode → 1 director per row

    Returns:
    --------
    pd.DataFrame
        Long format for director aggregation
    """
    temp = df.copy()
    temp["director"] = temp["director"].fillna("")
    temp["director"] = temp["director"].str.split("|")
    return temp.explode("director")


def director_success():
    """
    Director performance leaderboard (post-explosion).

    Aggregates:
    - movie_count, total_revenue, mean_rating

    Returns:
    --------
    pd.DataFrame
        Directors ranked by total_revenue

    Pipeline:
    ---------
    explode_directors() → filter empty → groupby → pivot → sort
    """
    df = explode_directors()
    df = df[df["director"] != ""]

    results = df.groupby("director").agg({
        "id": "count",
        "revenue": "sum",
        "vote_average": "mean"
    })

    results.columns = [
        "movie_count",
        "total_revenue",
        "mean_rating"
    ]

    return results.sort_values(by="total_revenue", ascending=False)


def top_directors_by_metric(metric, top=10):
    """
    Flexible director ranking system.

    Parameters:
    -----------
    metric : str
        Ranking column ('movie_count', 'total_revenue', 'mean_rating')
    top : int, default 10

    Returns:
    --------
    pd.DataFrame
        Top N directors by metric
    """
    data = director_success()
    return data.sort_values(by=metric, ascending=False).head(top)


if __name__ == "__main__":
    """
    Demo director analytics.
    """
    print('Top Directors by Number of Movies Directed: ',
          top_directors_by_metric('movie_count'))
    print('Top Directors by Revenue: ', top_directors_by_metric('total_revenue'))
    print('Top Directors by Rating: ', top_directors_by_metric('mean_rating'))
