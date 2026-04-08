import streamlit as st
from src.dashboard.visualizations import (
    plot_revenue_vs_budget,
    plot_roi_by_genre,
    plot_popularity_vs_rating,
    plot_yearly_box_office,
    plot_franchise_vs_standalone
)


# Page config
st.set_page_config(
    page_title="Movie Analytics Dashboard",
    layout="wide",
)

# Title (centered + bold)
st.markdown(
    "<h1 style='text-align: center; font-weight: bold;'>🎬 Movie Analytics Dashboard</h1>",
    unsafe_allow_html=True
)

st.markdown(
    "<p style='text-align: center;'>Explore performance trends, ROI, popularity, and franchise comparisons.</p>",
    unsafe_allow_html=True
)

st.markdown("---")

# Helper for consistent figure display


def display_plot(fig_func, title):
    st.markdown(
        f"<h3 style='text-align: center; font-weight: bold;'>{title}</h3>",
        unsafe_allow_html=True
    )
    fig = fig_func()
    st.pyplot(fig, use_container_width=True)


# =========================
# ROW 1: Revenue & ROI
# =========================
col1, col2 = st.columns(2)

with col1:
    display_plot(plot_revenue_vs_budget, "Revenue vs Budget")

with col2:
    display_plot(plot_roi_by_genre, "ROI Distribution by Genre")


# =========================
# ROW 2: Popularity & Yearly
# =========================
col3, col4 = st.columns(2)

with col3:
    display_plot(plot_popularity_vs_rating, "Popularity vs Rating")

with col4:
    display_plot(plot_yearly_box_office, "Yearly Box Office Performance")


# =========================
# ROW 3: Full-width section
# =========================
st.markdown("---")

display_plot(plot_franchise_vs_standalone, "Franchise vs Standalone Movies")
