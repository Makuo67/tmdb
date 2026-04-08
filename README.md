# TMDB Movie Analytics Pipeline

## Data Engineering Project Overview

**Author:** Okeke Makuochukwu  
**Purpose:** End-to-end ETL pipeline for TMDB movie data analysis focusing on financial performance (ROI/profit), KPIs, franchise insights, visualizations, and interactive dashboard.  
**Dataset:** TMDB (~20 high-profile movies like Avengers, Avatar) with budget/revenue in **$ millions USD**.

## Architecture

```
tmdb_api.py (API Fetch)
     ↓
pre_process.py (ETL: Clean + Feature Engineering)
     ↓
processed_movies.csv (23 columns: title/budget/revenue/genres/cast/director/ROI features)
     ↓
├── kpi.py + movie_performance.py (KPIs: rankings/franchise/director success)
├── search_movies.py (Search utilities)
├── visualizations.py (5 Matplotlib plots)
└── dashboard.py (Streamlit interactive dashboard)
    ↓
images/ (Generated plots)
```

## Key Features

- **ETL Pipeline:** API → JSON parsing → Pipe-delimited strings (genres/cast) → Numeric cleaning → Feature eng (cast_size/crew_size/director extraction).
- **Financial Metrics:** Profit (revenue-budget), ROI (revenue/budget), filtered rankings (min budget/votes).
- **Insights:** Franchise vs standalone, top directors/franchises by revenue/rating/movie count.
- **Visuals:** Revenue-budget scatter, ROI by genre boxplot, popularity-rating regression, yearly trends, franchise comparison.
- **Dashboard:** Streamlit app with vote filters displaying all plots.

## Data Schema (processed_movies.csv)

| Column                | Type       | Description                 |
| --------------------- | ---------- | --------------------------- |
| id                    | int        | TMDB movie ID               |
| title                 | str        | Movie title                 |
| budget                | float ($M) | Production budget           |
| revenue               | float ($M) | Box office revenue          |
| genres                | str        | \|-delimited genres         |
| cast                  | str        | \|-delimited top cast       |
| director              | str        | Primary director            |
| vote_average          | float      | Avg rating (vote_count ≥10) |
| popularity            | float      | TMDB popularity score       |
| belongs_to_collection | str        | Franchise name or 'None'    |
| ... (+17 more)        | ...        | release_date, runtime, etc. |

**Sample:** Avengers: Endgame - budget $356M, revenue $2799M, ROI ~7.86x

## Quick Start

1. Set `TMDB_API_KEY` in `.env`
2. Run ETL: `cd tmdb && python pre_process.py`
3. View KPIs: `python kpi.py` or `python movie_performance.py`
4. Generate plots: `python visualizations.py` (manual) or via dashboard
5. Launch Dashboard: `streamlit run dashboard.py`

## Key Insights (from Pipeline)

- **Top ROI:** High-budget blockbusters (Avengers series dominate).
- **Franchises outperform** standalone in revenue/ROI/popularity.
- **Directors:** Christopher Nolan/James Cameron lead revenue; ratings more varied.
- **Trends:** Revenue correlates positively with budget/popularity.

## Dependencies

```
pandas, requests, streamlit, matplotlib, seaborn, python-dotenv
pip install -r requirements.txt  # Create if needed
```

## Usage Examples

```python
# Re-run ETL
from pre_process import full_pipeline
df = full_pipeline()

# Top 5 ROI
from kpi import highest_roi
print(highest_roi(df))

# Franchise analysis
from movie_performance import franchise_success
print(franchise_success())
```

## File Documentation

See inline **docstrings** in each `.py` file for detailed function specs.

**Final Report:** See `FinalReport.md` for exhaustive analysis.

---

_Generated: Data Engineering Best Practices - Modular, documented, reproducible pipeline._
