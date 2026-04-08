# from visualizations import plot_revenue_vs_budget, plot_roi_by_genre, plot_popularity_vs_rating, plot_yearly_box_office, plot_franchise_vs_standalone


# def dashboard():
#     plot_revenue_vs_budget()
#     plot_roi_by_genre()
#     plot_popularity_vs_rating()
#     plot_yearly_box_office()
#     plot_franchise_vs_standalone()


# if __name__ == "__main__":
#     print(dashboard())


import streamlit as st
import pandas as pd
from visualizations import (
    plot_revenue_vs_budget,
    plot_roi_by_genre,
    plot_popularity_vs_rating,
    plot_yearly_box_office,
    plot_franchise_vs_standalone
)

"""
dashboard.py - Streamlit Movie Analytics Web App

Purpose:
Interactive dashboard displaying all 5 visualizations with vote filtering.
Production-ready Streamlit app with wide layout and sidebar controls.

Features:
- Vote count slider filter (0 to max)
- 5-section layout (revenue-budget, ROI genre, popularity-rating, 
  yearly trend, franchise comparison)
- Real-time dataset size display
- Publication-ready plots from visualizations.py

Run:
```bash
streamlit run tmdb/dashboard.py
```

Dependencies:
- streamlit, pandas
- visualizations.py + processed_movies.csv
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


filepath = 'data/processed_movies.csv'


def load_csv(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Unexpected error while loading CSV: {e}")

    return df


df = load_csv(filepath)


st.set_page_config(page_title="Movie Analytics Dashboard", layout="wide")

st.title("🎬 Movie Analytics Dashboard")
st.markdown(
    "Explore performance trends, ROI, popularity, and franchise comparisons.")

# Sidebar filters
st.sidebar.header("Filters")
min_votes = st.sidebar.slider(
    "Minimum Votes", 0, int(df["vote_count"].max()), 0)
filtered_df = df[df["vote_count"] >= min_votes]

st.sidebar.write("Movies in dataset:", filtered_df.shape[0])

# Layout sections
st.header("Revenue vs Budget")
st.pyplot(plot_revenue_vs_budget())

st.header("ROI Distribution by Genre")
st.pyplot(plot_roi_by_genre())

st.header("Popularity vs Rating")
st.pyplot(plot_popularity_vs_rating())

st.header("Yearly Box Office Performance")
st.pyplot(plot_yearly_box_office())

st.header("Franchise vs Standalone Movies")
st.pyplot(plot_franchise_vs_standalone())
