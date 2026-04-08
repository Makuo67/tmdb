import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

"""
visualizations.py - Movie Analytics Dashboard Plots

Purpose:
Generate 5 publication-ready Matplotlib/Seaborn plots for Streamlit dashboard.
Feature engineering for plotting (ROI, explode genres, movie_type).

Plots:
1. Revenue vs Budget scatter (success correlation)
2. ROI boxplot by genre (genre profitability)
3. Popularity vs Rating regression (quality-popularity link)
4. Yearly revenue trend line
5. Franchise vs Standalone bar chart

Dependencies:
- matplotlib, seaborn, pandas
- processed_movies.csv

Usage:
```python
from visualizations import plot_revenue_vs_budget
fig = plot_revenue_vs_budget()
```
"""

filepath = 'processed_movies.csv'


def load_csv(filepath):
    """
    CSV loader for plotting context.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Unexpected error while loading CSV: {e}")

    return df


df = load_csv(filepath)


# Convert date column to datetime
df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

# Calculate ROI, avoid zeros
df["roi"] = df.apply(
    lambda r: (r["revenue"] / r["budget"]
               ) if r["budget"] and r["budget"] != 0 else pd.NA,
    axis=1
)

# Identify franchise vs standalone
df["movie_type"] = df["belongs_to_collection"].apply(
    lambda x: "Franchise" if pd.notna(x) else "Standalone"
)


# Revenue vs Budget
def plot_revenue_vs_budget():
    """
    Revenue vs Budget scatter plot.

    Visualizes core Hollywood success equation.

    Returns:
    --------
    matplotlib.figure.Figure
        Ready for Streamlit st.pyplot()

    Insights:
    - Strong positive correlation expected
    - Outliers = breakout hits
    """
    fig, ax = plt.subplots()
    ax.scatter(df["budget"], df["revenue"], alpha=0.7)
    ax.set_title("Revenue vs Budget")
    ax.set_xlabel("Budget ($)")
    ax.set_ylabel("Revenue ($)")
    return fig

# ROI Distribution by Genre

# First, we explode genre


def explode_genres():
    """
    Normalize genres for boxplot analysis (pipe → 1/row).
    """
    df_copy = df.copy()
    df_copy["genres"] = df_copy["genres"].str.split("|")
    return df_copy.explode("genres")


def plot_roi_by_genre():
    """
    Genre profitability boxplot (post-explosion).

    Returns:
    --------
    matplotlib.figure.Figure
        ROI distribution by genre

    Insights:
    - Animation/Adventure typically highest ROI
    - Horror variable but high-upside
    """
    genre_df = explode_genres()

    fig, ax = plt.subplots()
    sns.boxplot(data=genre_df, x="genres", y="roi", ax=ax)
    ax.set_title("ROI by Genre")
    ax.tick_params(axis="x", rotation=45)
    return fig


# Popularity vs Rating

def plot_popularity_vs_rating():
    """
    Rating vs Popularity regression plot.

    Tests quality-popularity hypothesis.

    Returns:
    --------
    matplotlib.figure.Figure
        Regression with confidence interval

    Insights:
    - Weak positive correlation typical
    - Blockbusters sometimes rate lower
    """
    fig, ax = plt.subplots()
    sns.regplot(data=df, x="vote_average", y="popularity",
                scatter_kws={"alpha": 0.6}, ax=ax)
    ax.set_title("Popularity vs Rating")
    return fig


# Yearly Box Office Trends
def plot_yearly_box_office():
    """
    Average annual box office revenue trend.

    Returns:
    --------
    matplotlib.figure.Figure
        Line plot with markers

    Insights:
    - Inflation-adjusted trend upward
    - Blockbuster era acceleration
    """
    fig, ax = plt.subplots()
    df["year"] = df["release_date"].dt.year
    yearly = df.groupby("year")["revenue"].mean()
    ax.plot(yearly.index, yearly.values, marker="o")
    ax.set_title("Yearly Box Office Revenue")
    return fig


# Franchise Vs Standalone
def plot_franchise_vs_standalone():
    """
    Franchise advantage bar chart (4 metrics).

    Returns:
    --------
    matplotlib.figure.Figure
        Grouped bar chart

    Insights:
    - Franchises dominate revenue/ROI/popularity
    - Ratings comparable (sequel dilution?)
    """
    fig, ax = plt.subplots()
    compare = df.groupby("movie_type").agg({
        "revenue": "mean",
        "roi": "mean",
        "popularity": "mean",
        "vote_average": "mean"
    })
    compare.plot(kind="bar", ax=ax)
    ax.set_title("Franchise vs Standalone Movies")
    return fig
