"""TfL network dashboard: the entry point. Sets up the page and the two pages in the sidebar."""

import streamlit as st

st.set_page_config(page_title="TfL network dashboard", layout="wide")

page = st.navigation(
    [
        st.Page("one_day.py", title="One day", icon=":material/today:", default=True),
        st.Page("trends.py", title="Trends", icon=":material/show_chart:"),
    ]
)
page.run()
