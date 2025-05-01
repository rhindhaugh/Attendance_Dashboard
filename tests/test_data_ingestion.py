import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data_ingestion import (
    load_key_card_data,
    load_employee_info,
    load_employment_history,
    calculate_default_date_range,
    merge_key_card_data
)

class TestDataIngestion(unittest.TestCase):
    
    def setUp(self):
        """Set up test data for each test."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        
        # Create sample key card data
        self.key_card_data1 = pd.DataFrame({
            'Date/time': ['01/03/2024 09:15', '01/03/2024 08:30', '02/03/2024 10:00'],
            'User': ['123 Doe, John', '456 Smith, Jane', '789 Brown, Mark'],
            'Token number': ['T123', 'T456', 'T789'],
            'Where': ['Main Entrance', 'Side Entrance', 'Main Entrance'],
            'Event': ['Valid Access', 'Valid Access', 'Valid Access'],
            'Details': ['', '', '']
        })
        
        # Create second sample with overlap and new data
        self.key_card_data2 = pd.DataFrame({
            'Date/time': ['01/03/2024 09:15', '03/03/2024 08:45', '04/03/2024 11:00'],
            'User': ['123 Doe, John', '456 Smith, Jane', '321 Green, Sarah'],
            'Token number': ['T123', 'T456', 'T321'],
            'Where': ['Main Entrance', 'Side Entrance', 'Main Entrance'],
            'Event': ['Valid Access', 'Valid Access', 'Valid Access'],
            'Details': ['', '', '']
        })
        
        # Save test files
        self.key_card_path1 = self.temp_path / 'key_card_access1.csv'
        self.key_card_path2 = self.temp_path / 'key_card_access2.csv'
        
        self.key_card_data1.to_csv(self.key_card_path1, index=False)
        self.key_card_data2.to_csv(self.key_card_path2, index=False)
    
    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()
    
    def test_load_key_card_data(self):
        """Test loading key card data."""
        df = load_key_card_data(str(self.key_card_path1))
        self.assertEqual(len(df), 3)
        self.assertIn('Date/time', df.columns)
        self.assertIn('User', df.columns)
    
    def test_load_key_card_data_with_date_filter(self):
        """Test loading key card data with date filtering."""
        # Convert '01/03/2024' to '2024-03-01' format
        df = load_key_card_data(
            str(self.key_card_path1),
            start_date='2024-03-02',
            end_date='2024-03-04'
        )
        self.assertEqual(len(df), 1)  # Only one record on March 2nd
    
    def test_calculate_default_date_range(self):
        """Test calculating default date range."""
        start_date, end_date = calculate_default_date_range(days=7)
        
        # Parse dates back to datetime for comparison
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Check that end date is today
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(end_dt.date(), today.date())
        
        # Check that start date is 7 days before end date
        expected_start = today - timedelta(days=7)
        self.assertEqual(start_dt.date(), expected_start.date())
    
    def test_merge_key_card_data(self):
        """Test merging key card data files."""
        # Set output path for merged data
        output_path = self.temp_path / 'merged_key_card.csv'
        
        # Merge the data
        merged_df = merge_key_card_data(
            str(self.key_card_path1),
            str(self.key_card_path2),
            str(output_path),
            create_backup=False
        )
        
        # Check that the merged data has the expected number of rows (5 unique records)
        self.assertEqual(len(merged_df), 5)
        
        # Check that the duplicate row was removed
        # There should be only one record for Doe, John on 01/03/2024 09:15
        dup_check = merged_df[
            (merged_df['User'] == '123 Doe, John') & 
            (merged_df['Date/time'] == '01/03/2024 09:15')
        ]
        self.assertEqual(len(dup_check), 1)
        
        # Check that the output file was created and has the correct number of rows
        self.assertTrue(output_path.exists())
        loaded_df = pd.read_csv(output_path)
        self.assertEqual(len(loaded_df), 5)
    
    def test_merge_key_card_data_missing_file(self):
        """Test merging when file is missing."""
        # Test with non-existent path
        nonexistent_path = self.temp_path / 'nonexistent.csv'
        
        # Should return empty DataFrame when file is missing
        result_df = merge_key_card_data(
            str(nonexistent_path),
            str(self.key_card_path2)
        )
        
        self.assertTrue(result_df.empty)

if __name__ == '__main__':
    unittest.main()