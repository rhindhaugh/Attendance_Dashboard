import streamlit as st
import pandas as pd

from data_ingestion import load_key_card_data, load_employee_info
from data_cleaning import clean_key_card_data, clean_employee_info, merge_key_card_with_employee_info
from data_analysis import calculate_visit_counts, calculate_average_arrival_hour

def main():
    st.title("Office Attendance Dashboard")

    # Step 1: Load Data
    key_card_df = load_key_card_data("data/raw/key_card_access.csv")
    employee_df = load_employee_info("data/raw/employee_info.csv")

    # Step 2: Clean & merge
    key_card_df = clean_key_card_data(key_card_df)
    employee_df = clean_employee_info(employee_df)
    merged_df = merge_key_card_with_employee_info(key_card_df, employee_df)

    # Step 3: Analysis
    visit_counts = calculate_visit_counts(merged_df)
    avg_arrivals = calculate_average_arrival_hour(merged_df)

    # Step 4: Display results
    st.subheader("Visit Counts by Employee ID")
    st.dataframe(visit_counts)

    st.subheader("Average Arrival Hour by Employee ID")
    st.dataframe(avg_arrivals)

if __name__ == "__main__":
    main()
