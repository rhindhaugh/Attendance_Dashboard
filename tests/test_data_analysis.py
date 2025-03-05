import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data_analysis.attendance_table import build_attendance_table
from src.data_analysis.common import (
    get_employment_date_mask,
    get_london_hybrid_ft_mask,
    calculate_eligible_employees,
    calculate_present_employees
)

class TestDataAnalysis(unittest.TestCase):
    
    def setUp(self):
        """Set up test data for each test."""
        # Create a test dataframe with employee and attendance data
        self.combined_df = pd.DataFrame({
            'employee_id': [123, 123, 456, 456, 849],
            'Last name, First name': ['Doe, John', 'Doe, John', 'Smith, Jane', 'Smith, Jane', 'Hindhaugh, Robert'],
            'Date/time': pd.to_datetime(['2024-03-01 09:15', '2024-03-02 08:30', '2024-03-01 10:00', 
                                        '2024-03-03 09:45', '2024-03-02 08:00']),
            'date_only': pd.to_datetime(['2024-03-01', '2024-03-02', '2024-03-01', '2024-03-03', '2024-03-02']).date,
            'Event': ['Valid Access', 'Valid Access', 'Valid Access', 'Valid Access', 'Valid Access'],
            'Where': ['Main Entrance', 'Side Entrance', 'Main Entrance', 'Side Entrance', 'Main Entrance'],
            'Location': ['London UK', 'London UK', 'Paris FR', 'Paris FR', 'London UK'],
            'Working Status': ['Hybrid', 'Hybrid', 'Office', 'Office', 'Hybrid'],
            'is_full_time': [True, True, False, False, True],
            'parsed_time': pd.to_datetime(['2024-03-01 09:15', '2024-03-02 08:30', '2024-03-01 10:00', 
                                         '2024-03-03 09:45', '2024-03-02 08:00']),
            'Combined hire date': pd.to_datetime(['2022-01-01', '2022-01-01', '2022-02-15', 
                                                 '2022-02-15', '2022-01-03']),
            'Most recent day worked': [None, None, None, None, None]  # All active
        })
        
    def test_build_attendance_table(self):
        """Test the build_attendance_table function."""
        # Call the function
        result = build_attendance_table(self.combined_df)
        
        # Check that the result has the expected shape
        unique_employees = self.combined_df['employee_id'].nunique()
        unique_dates = self.combined_df['date_only'].nunique()
        expected_rows = unique_employees * unique_dates
        self.assertEqual(len(result), expected_rows)
        
        # Check that is_present is correctly calculated
        present_days = result[result['is_present'] == True]['date_only'].nunique()
        self.assertEqual(present_days, unique_dates)
        
        # Check that days_attended is correctly calculated
        john_mask = result['employee_name'] == 'Doe, John'
        self.assertEqual(result.loc[john_mask, 'days_attended'].iloc[0], 2)
    
    def test_get_employment_date_mask(self):
        """Test the get_employment_date_mask function."""
        # Create a test date
        test_date = pd.Timestamp('2023-01-01')
        
        # Call the function
        mask = get_employment_date_mask(self.combined_df, test_date)
        
        # All employees should be employed on this date
        self.assertTrue(mask.all())
        
        # Test with an earlier date (before any employment)
        early_date = pd.Timestamp('2021-01-01')
        early_mask = get_employment_date_mask(self.combined_df, early_date)
        self.assertFalse(early_mask.any())
    
    def test_get_london_hybrid_ft_mask(self):
        """Test the get_london_hybrid_ft_mask function."""
        # Call the function
        mask = get_london_hybrid_ft_mask(self.combined_df)
        
        # Check the mask (John and Robert match, Jane doesn't)
        expected = [True, True, False, False, True]
        self.assertEqual(mask.tolist(), expected)
    
    def test_calculate_eligible_employees(self):
        """Test the calculate_eligible_employees function."""
        # Test date when all should be employed
        test_date = pd.Timestamp('2023-01-01')
        
        # Call the function
        eligible_count = calculate_eligible_employees(self.combined_df, test_date)
        
        # Expect 2 employees (Doe and Hindhaugh - London, Hybrid, Full-Time)
        self.assertEqual(eligible_count, 2)
    
    def test_calculate_present_employees(self):
        """Test the calculate_present_employees function."""
        # Test with a specific date
        test_date = pd.Timestamp('2024-03-01').date()
        
        # Call the function
        present_lhft = calculate_present_employees(self.combined_df, test_date, lhft_only=True)
        present_all = calculate_present_employees(self.combined_df, test_date, lhft_only=False)
        
        # Expect 1 LHFT employee (John) and 2 total employees (John and Jane)
        self.assertEqual(present_lhft, 1)
        self.assertEqual(present_all, 2)

if __name__ == '__main__':
    unittest.main()