import pandas as pd
import os

"""
kpi.py - Movie Financial KPI Computation Engine

Purpose:
Compute comprehensive financial performance metrics and rankings from processed CSV.
Industry-standard KPIs for movie analytics (ROI, profit, ratings with quality filters).

Key Features:
- Vectorized metric calculation (profit/ROI)
- Parameterized ranking engine with business filters
- Top/bottom-N analysis across 9+ dimensions
- Production-grade error handling for missing data

Dependencies:
- pandas
- processed_movies.csv (from pre_process.py)

Example:
```python
from kpi import highest_roi, df
print(highest_roi(df, top=5))  # Top 5 ROI movies (budget ≥$10M)
```
"""

filepath = 'processed_movies.csv'


def load_csv(filepath):
    """
    Production CSV loader with existence and error handling.

    Parameters:
    -----------
    filepath : str
        Path to processed_movies.csv

    Returns:
    --------
    pd.DataFrame
        Loaded dataset

    Raises:
    -------
    FileNotFoundError
        If CSV missing (run pre_process.py first)
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Unexpected error while loading CSV: {e}")

    return df


def profit(revenue, budget):
    """
    Compute absolute profit margin ($M).

    Parameters:
    -----------
    revenue : float
        Box office revenue ($M)
    budget : float
        Production budget ($M)

    Returns:
    --------
    float or None
        revenue - budget or None if missing data
    """
    if revenue is None or budget is None:
        return None

    return revenue - budget


def roi(revenue, budget):
    """
    Return on Investment ratio (revenue/budget).

    Parameters:
    -----------
    revenue : float
        Box office revenue ($M)
    budget : float
        Production budget ($M)

    Returns:
    --------
    float or None
        ROI multiplier (e.g., 7.86x) or None if budget=0/missing

    Example:
    --------
    >>> roi(2799, 356)  # Avengers Endgame ~7.86
    7.86
    """
    if revenue is None or budget is None or budget == 0:
        return None

    return revenue / budget


def rank_movies(df, column, ascending=False, limit=5, filter_condition=None):
    """
    Generic ranking engine with business logic filters.

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    column : str
        Ranking metric ('revenue', 'roi', 'vote_average', etc.)
    ascending : bool, default False
        True for lowest, False for highest
    limit : int, default 5
        N top/bottom records
    filter_condition : callable, optional
        Business filter e.g. lambda d: d['budget'] >= 10

    Returns:
    --------
    pd.DataFrame
        Filtered + sorted + head(N) results

    Example:
    --------
    >>> rank_movies(df, 'vote_average', filter_condition=lambda d: d['vote_count']>=10)
    """
    temp = df.copy()

    if filter_condition is not None:
        temp = temp[filter_condition(temp)]
    return temp.sort_values(by=column, ascending=ascending).head(limit)


# ===========================================================
df = load_csv(filepath)


def highest_revenue(df, top=10):
    """
    Top movies by box office revenue.

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 10
        Number of records

    Returns:
    --------
    pd.DataFrame
        Top revenue movies
    """
    return rank_movies(df, column="revenue", ascending=False, limit=top)


def highest_budget(df, top=5):
    """
    Biggest production budgets.

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Highest budget movies
    """
    return rank_movies(df, column="budget", ascending=False, limit=top)


def highest_profit(df, top=5):
    """
    Most profitable movies (revenue - budget).

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Top profit movies (temporary 'profit' column added)
    """
    df["profit"] = df["revenue"] - df["budget"]
    return rank_movies(df, "profit", ascending=False, limit=top)


def lowest_profit(df, top=5):
    """
    Biggest money losers (lowest profit).

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Worst performing movies financially
    """
    df["profit"] = df["revenue"] - df["budget"]
    return rank_movies(df, "profit", ascending=True, limit=top)


def highest_roi(df, top=10):
    """
    Best ROI performers (revenue/budget ratio, budget ≥$10M).

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 10

    Returns:
    --------
    pd.DataFrame
        Top ROI movies (quality filter applied)

    Notes:
    ------
    - Filters budget ≥$10M (business relevant)
    - Temporary 'roi' column computed
    """
    df["roi"] = df["revenue"] / df["budget"]
    return rank_movies(
        df,
        column="roi",
        ascending=False,
        limit=top,
        filter_condition=lambda d: d["budget"] >= 10
    )


def lowest_roi(df, top=10):
    df["roi"] = df["revenue"] / df["budget"]
    return rank_movies(
        df,
        column="roi",
        ascending=True,
        limit=top,
        filter_condition=lambda d: d["budget"] >= 10
    )


def most_voted(df, top=5):
    """
    Most popular by audience votes.

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Top vote_count movies
    """
    return rank_movies(df, column="vote_count", ascending=False, limit=top)


def highest_rated(df, top=5):
    """
    Best rated movies (vote_count ≥10 filter).

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Highest vote_average (quality filtered)
    """
    return rank_movies(
        df,
        column="vote_average",
        ascending=False,
        limit=top,
        filter_condition=lambda d: d["vote_count"] >= 10
    )


def lowest_rated(df, top=5):
    """
    Worst rated movies (vote_count ≥10 filter).

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Lowest vote_average (quality filtered)
    """
    return rank_movies(
        df,
        column="vote_average",
        ascending=True,
        limit=top,
        filter_condition=lambda d: d["vote_count"] >= 10
    )


def most_popular(df, top=5):
    """
    TMDB popularity leaders.

    Parameters:
    -----------
    df : pd.DataFrame
        Movie dataset
    top : int, default 5

    Returns:
    --------
    pd.DataFrame
        Top popularity score movies
    """
    return rank_movies(df, column="popularity", ascending=False, limit=top)


if __name__ == "__main__":
    """
    Production demo: Print all KPIs for quick insights.
    """
    print("================ Highest by Budget ==================================")
    print(highest_budget(df))
    print("================ Highest by Revenue ================================")
    print(highest_revenue(df))
    print("================ Highest by Profit =================================")
    print(highest_profit(df))
    print("================ Lowest by Profit =================================")
    print(lowest_profit(df))
    print("================ Highest by ROI =================================")
    print(highest_roi(df))
    print("================ Lowest by ROI =================================")
    print(lowest_roi(df))
    print("================ Most Voted =================================")
    print(most_voted(df))
    print("================ Highest Rated =================================")
    print(highest_rated(df))
    print("================ Lowest Rated =================================")
    print(lowest_rated(df))
    print("================ Most popular =================================")
    print(most_popular(df))
