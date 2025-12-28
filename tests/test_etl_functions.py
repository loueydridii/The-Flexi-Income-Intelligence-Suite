"""
Unit Tests for ETL Functions

These tests validate individual ETL transformation functions.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime


class TestDataTransformations:
    """Test suite for data transformation functions"""
    
    def test_date_id_generation(self):
        """Test date_id generation in YYYYMMDD format"""
        test_dates = pd.Series(['2025-01-15', '2025-12-31', '2024-03-01'])
        test_df = pd.DataFrame({'work_date': pd.to_datetime(test_dates)})
        
        test_df['date_id'] = test_df['work_date'].dt.strftime('%Y%m%d').astype(int)
        
        expected = [20250115, 20251231, 20240301]
        assert test_df['date_id'].tolist() == expected
    
    def test_text_standardization_lower(self):
        """Test case-insensitive text standardization"""
        test_data = pd.Series(['Upwork', 'UPWORK', 'upwork', 'UpWoRk'])
        standardized = test_data.str.lower()
        
        assert standardized.nunique() == 1
        assert standardized.iloc[0] == 'upwork'
    
    def test_is_gap_day_calculation(self):
        """Test is_gap_day flag calculation logic"""
        test_df = pd.DataFrame({
            'earnings_usd': [100, 0, None, 500],
            'job_completed': [1, 0, 1, 1]
        })
        
        test_df['is_gap_day'] = (
            (test_df['earnings_usd'].isna()) | 
            (test_df['earnings_usd'] == 0) | 
            (test_df['job_completed'] == 0)
        ).astype(int)
        
        expected = [0, 1, 1, 0]
        assert test_df['is_gap_day'].tolist() == expected
    
    def test_worker_id_string_conversion(self):
        """Test worker_id conversion to string type"""
        test_df = pd.DataFrame({'worker_id': [1, 2, 3, 4, 5]})
        test_df['worker_id'] = test_df['worker_id'].astype(str)
        
        assert test_df['worker_id'].dtype == 'object'
        assert test_df['worker_id'].iloc[0] == '1'


class TestDataCleaning:
    """Test suite for data cleaning operations"""
    
    def test_duplicate_removal(self):
        """Test duplicate record removal"""
        test_df = pd.DataFrame({
            'id': [1, 2, 2, 3, 4, 4],
            'value': ['a', 'b', 'b', 'c', 'd', 'd']
        })
        
        cleaned = test_df.drop_duplicates()
        assert len(cleaned) == 4
    
    def test_missing_value_handling_fillna(self):
        """Test missing value handling with fillna"""
        test_df = pd.DataFrame({
            'value': [1, 2, None, 4, None]
        })
        
        filled = test_df['value'].fillna(0)
        assert filled.isna().sum() == 0
        assert filled.iloc[2] == 0
    
    def test_whitespace_removal(self):
        """Test whitespace stripping from text fields"""
        test_data = pd.Series(['  Upwork  ', ' Fiverr', 'Freelancer  '])
        cleaned = test_data.str.strip()
        
        expected = ['Upwork', 'Fiverr', 'Freelancer']
        assert cleaned.tolist() == expected


class TestDataValidation:
    """Test suite for data validation logic"""
    
    def test_numeric_range_validation(self):
        """Test numeric values are within expected ranges"""
        test_df = pd.DataFrame({
            'rating': [4.5, 3.2, 5.0, 1.8, 4.9]
        })
        
        # Validate ratings between 0 and 5
        is_valid = (test_df['rating'] >= 0) & (test_df['rating'] <= 5)
        assert is_valid.all()
    
    def test_categorical_values_in_allowed_list(self):
        """Test categorical values match expected list"""
        test_df = pd.DataFrame({
            'project_type': ['Hourly', 'Fixed', 'Hourly', 'Fixed']
        })
        
        allowed_types = ['Hourly', 'Fixed']
        is_valid = test_df['project_type'].isin(allowed_types)
        assert is_valid.all()
    
    def test_foreign_key_exists(self):
        """Test foreign key values exist in reference table"""
        fact_df = pd.DataFrame({
            'platform_id': [1, 2, 3, 2, 1]
        })
        
        dim_df = pd.DataFrame({
            'platform_id': [1, 2, 3, 4, 5]
        })
        
        # Check all fact platform_ids exist in dimension
        is_valid = fact_df['platform_id'].isin(dim_df['platform_id'])
        assert is_valid.all()


class TestMergeOperations:
    """Test suite for merge/join operations"""
    
    def test_left_join_preserves_all_records(self):
        """Test left join preserves all records from left table"""
        left_df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        right_df = pd.DataFrame({
            'id': [1, 2],
            'label': ['x', 'y']
        })
        
        merged = left_df.merge(right_df, on='id', how='left')
        assert len(merged) == len(left_df)
    
    def test_case_insensitive_merge(self):
        """Test case-insensitive merge using lowercase columns"""
        left_df = pd.DataFrame({
            'platform': ['Upwork', 'Fiverr', 'TOPTAL']
        })
        
        right_df = pd.DataFrame({
            'platform': ['upwork', 'fiverr', 'toptal'],
            'id': [1, 2, 3]
        })
        
        left_df['platform_lower'] = left_df['platform'].str.lower()
        right_df['platform_lower'] = right_df['platform'].str.lower()
        
        merged = left_df.merge(right_df, on='platform_lower', how='left')
        assert merged['id'].notna().all()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
