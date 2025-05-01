import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data_cleaning import (
    clean_key_card_data,
    clean_employee_info, 
    add_full_time_indicators,
    merge_key_card_with_employee_info,
    add_time_analysis_columns,
    normalize_compliance_divisions
)

class TestDataCleaning(unittest.TestCase):
    
    def setUp(self):
        """Set up test data for each test."""
        # Create a simple key card test dataframe
        self.key_card_df = pd.DataFrame({
            'User': ['123 Doe, John', '456 Smith, Jane', 'Hindhaugh, Robert'],
            'Date/time': ['01/03/2024 09:15', '01/03/2024 08:30', '01/03/2024 10:00'],
            'Event': ['Valid Access', 'Valid Access', 'Valid Access'],
            'Where': ['Main Entrance', 'Side Entrance', 'Main Entrance']
        })
        
        # Create a simple employee test dataframe
        self.employee_df = pd.DataFrame({
            'employee_id': [123, 456, 849],
            'Last name, First name': ['Doe, John', 'Smith, Jane', 'Hindhaugh, Robert'],
            'Hire Date': ['01/01/2022', '15/02/2022', '03/01/2022'],
            'Location': ['London UK', 'Paris FR', 'London UK'],
            'Working Status': ['Hybrid', 'Office', 'Hybrid'],
            'Status': ['Active', 'Active', 'Active'],
            'Division': ['Operations', 'Finance', 'Technology']
        })
        
        # Create a test dataframe with compliance divisions
        self.compliance_df = pd.DataFrame({
            'Division': [
                'Compliance - UK',
                'Compliance - US',
                'Compliance',
                'Operations',
                'Finance',
                None,
                np.nan
            ]
        })
        
        # Create a simple status history lookup
        self.status_lookup = {
            123: [(pd.Timestamp('2022-01-01'), 'Full-Time')],
            456: [(pd.Timestamp('2022-02-15'), 'Part-Time')],
            849: [(pd.Timestamp('2022-01-03'), 'Full-Time')]
        }
    
    def test_clean_key_card_data(self):
        """Test the clean_key_card_data function."""
        # Call the function
        result = clean_key_card_data(self.key_card_df)
        
        # Check that employee_id was extracted correctly
        self.assertEqual(result['employee_id'].tolist(), [123.0, 456.0, 849.0])
        
        # Check that parsed_time was added
        self.assertTrue('parsed_time' in result.columns)
        
        # Check that date_only was added
        self.assertTrue('date_only' in result.columns)
    
    def test_clean_employee_info(self):
        """Test the clean_employee_info function."""
        # Create a test dataframe with the required columns for clean_employee_info
        test_df = pd.DataFrame({
            'Employee #': ['123', '456', '849'],
            'Last name, First name': ['Doe, John', 'Smith, Jane', 'Hindhaugh, Robert'],
            'Hire Date': ['01/01/2022', '15/02/2022', '03/01/2022'],
            'Location': ['London UK', 'Paris FR', 'London UK'],
            'Working Status': ['Hybrid', 'Office', 'Hybrid'],
            'Status': ['Active', 'Active', 'Active'],
            'Division': ['Operations', 'Compliance - UK', 'Compliance - US']
        })
        
        # Call the function
        result = clean_employee_info(test_df)
        
        # Check that Combined hire date was added
        self.assertTrue('Combined hire date' in result.columns)
        
        # Check that hire dates were converted to datetime
        self.assertTrue(pd.api.types.is_datetime64_dtype(result['Combined hire date']))
        
        # Check that compliance divisions were normalized
        self.assertEqual(result.loc[1, 'Division'], 'Compliance')
        self.assertEqual(result.loc[2, 'Division'], 'Compliance')
    
    def test_add_full_time_indicators(self):
        """Test the add_full_time_indicators function."""
        # Prepare data with required columns
        df = pd.DataFrame({
            'employee_id': [123.0, 456.0, 849.0],
            'User': ['123 Doe, John', '456 Smith, Jane', 'Hindhaugh, Robert'],
            'date_only': [pd.Timestamp('2024-03-01')] * 3
        })
        
        # Call the function
        result = add_full_time_indicators(df, self.status_lookup)
        
        # Check that is_full_time column was added
        self.assertTrue('is_full_time' in result.columns)
        
        # Robert Hindhaugh (ID 849) should always be full-time
        hindhaugh_mask = result['User'] == 'Hindhaugh, Robert'
        self.assertTrue(result.loc[hindhaugh_mask, 'is_full_time'].all())
    
    def test_add_time_analysis_columns(self):
        """Test the add_time_analysis_columns function."""
        # Set up the data
        df = self.key_card_df.copy()
        df['parsed_time'] = pd.to_datetime(df['Date/time'], dayfirst=True)
        
        # Call the function
        result = add_time_analysis_columns(df)
        
        # Check that hour column was added
        self.assertTrue('hour' in result.columns)
        
        # Check that hour values are correct
        self.assertEqual(result['hour'].tolist(), [9, 8, 10])
        
    @patch('src.data_cleaning.logger')
    def test_normalize_compliance_divisions(self, mock_logger):
        """Test the normalize_compliance_divisions function."""
        # Call the function
        result = normalize_compliance_divisions(self.compliance_df['Division'])
        
        # Check that all compliance divisions are normalized to 'Compliance'
        expected_values = [
            'Compliance',  # Was Compliance - UK
            'Compliance',  # Was Compliance - US
            'Compliance',  # Was already Compliance
            'Operations',  # Not a compliance division
            'Finance',     # Not a compliance division
            None,          # Null value should remain null
            np.nan         # NaN value should remain NaN
        ]
        
        # Convert result to a list for comparison, handling None and NaN values
        result_values = []
        for val in result:
            if pd.isna(val):
                if val is None:
                    result_values.append(None)
                else:
                    result_values.append(np.nan)
            else:
                result_values.append(val)
        
        # Check first 5 values (we can't directly compare None and NaN)
        for i in range(5):
            self.assertEqual(result_values[i], expected_values[i])
        
        # Check that None remains None
        self.assertIsNone(result_values[5])
        
        # Check that NaN remains NaN
        self.assertTrue(pd.isna(result_values[6]))
        
        # Verify logging
        mock_logger.info.assert_called_once_with("Normalized 3 compliance division entries")

if __name__ == '__main__':
    unittest.main()
