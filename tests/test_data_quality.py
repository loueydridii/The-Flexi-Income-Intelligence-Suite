"""
Data Quality Tests for The Flexi Income Intelligence Suite

These tests validate the quality and integrity of the data warehouse tables.
"""

import pytest
import pandas as pd
from pathlib import Path


# Define base paths
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_DIR = BASE_DIR / 'data_raw'
CLEANED_DATA_DIR = BASE_DIR / 'data_cleaned'


class TestDimensionTables:
    """Test suite for dimension table data quality"""
    
    def test_dim_worker_no_duplicates(self):
        """Verify dim_worker has no duplicate worker_ids"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_worker.csv')
        assert df['worker_id'].duplicated().sum() == 0, "Found duplicate worker_ids"
    
    def test_dim_worker_no_nulls_in_pk(self):
        """Verify dim_worker primary key has no null values"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_worker.csv')
        assert df['worker_id'].notna().all(), "Found null values in worker_id"
    
    def test_dim_platform_no_duplicates(self):
        """Verify dim_platform has no duplicate platform_ids"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_platform.csv')
        assert df['platform_id'].duplicated().sum() == 0, "Found duplicate platform_ids"
    
    def test_dim_platform_no_nulls_in_pk(self):
        """Verify dim_platform primary key has no null values"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_platform.csv')
        assert df['platform_id'].notna().all(), "Found null values in platform_id"
    
    def test_dim_region_no_duplicates(self):
        """Verify dim_region has no duplicate region_ids"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_region.csv')
        assert df['region_id'].duplicated().sum() == 0, "Found duplicate region_ids"
    
    def test_dim_project_no_duplicates(self):
        """Verify dim_project has no duplicate project_ids"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_project.csv')
        assert df['project_id'].duplicated().sum() == 0, "Found duplicate project_ids"
    
    def test_dim_date_no_duplicates(self):
        """Verify dim_date has no duplicate date_ids"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'dim_date.csv')
        assert df['date_id'].duplicated().sum() == 0, "Found duplicate date_ids"


class TestFactTable:
    """Test suite for fact table data quality"""
    
    def test_fact_table_has_records(self):
        """Verify fact table contains records"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        assert len(df) > 0, "Fact table is empty"
    
    def test_fact_no_duplicate_job_ids(self):
        """Verify fact table has no duplicate job_ids"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        assert df['job_id'].duplicated().sum() == 0, "Found duplicate job_ids"
    
    def test_fact_no_nulls_in_required_fields(self):
        """Verify required fields in fact table have no nulls"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        required_fields = ['job_id', 'worker_id', 'date_id']
        for field in required_fields:
            assert df[field].notna().all(), f"Found null values in required field: {field}"
    
    def test_fact_earnings_non_negative(self):
        """Verify earnings values are non-negative when present"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        earnings = df['earnings_usd'].dropna()
        assert (earnings >= 0).all(), "Found negative earnings values"
    
    def test_fact_client_rating_valid_range(self):
        """Verify client ratings are within valid range (typically 0-5)"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        ratings = df['client_rating'].dropna()
        assert (ratings >= 0).all() and (ratings <= 5).all(), "Client ratings outside valid range [0, 5]"
    
    def test_fact_job_success_rate_valid_range(self):
        """Verify job success rates are within valid percentage range (0-100)"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        success_rates = df['job_success_rate'].dropna()
        assert (success_rates >= 0).all() and (success_rates <= 100).all(), "Success rates outside valid range [0, 100]"


class TestReferentialIntegrity:
    """Test suite for referential integrity between fact and dimension tables"""
    
    def test_worker_id_integrity(self):
        """Verify all worker_ids in fact table exist in dim_worker"""
        fact = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        dim = pd.read_csv(CLEANED_DATA_DIR / 'dim_worker.csv')
        
        fact_workers = set(fact['worker_id'].dropna().astype(str))
        dim_workers = set(dim['worker_id'].astype(str))
        
        orphans = fact_workers - dim_workers
        assert len(orphans) == 0, f"Found {len(orphans)} worker_ids in fact table not in dimension"
    
    def test_platform_id_integrity(self):
        """Verify all platform_ids in fact table exist in dim_platform"""
        fact = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        dim = pd.read_csv(CLEANED_DATA_DIR / 'dim_platform.csv')
        
        fact_platforms = set(fact['platform_id'].dropna().astype(int))
        dim_platforms = set(dim['platform_id'].astype(int))
        
        orphans = fact_platforms - dim_platforms
        assert len(orphans) == 0, f"Found {len(orphans)} platform_ids in fact table not in dimension"
    
    def test_date_id_integrity(self):
        """Verify all date_ids in fact table exist in dim_date"""
        fact = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        dim = pd.read_csv(CLEANED_DATA_DIR / 'dim_date.csv')
        
        fact_dates = set(fact['date_id'].dropna().astype(int))
        dim_dates = set(dim['date_id'].astype(int))
        
        orphans = fact_dates - dim_dates
        assert len(orphans) == 0, f"Found {len(orphans)} date_ids in fact table not in dimension"


class TestDataConsistency:
    """Test suite for data consistency rules"""
    
    def test_hourly_rate_reasonable(self):
        """Verify hourly rates are within reasonable bounds"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        hourly_rates = df['hourly_rate'].dropna()
        
        # Assuming reasonable hourly rate range is $1 to $500
        assert (hourly_rates >= 1).all(), "Found hourly rates below $1"
        assert (hourly_rates <= 500).all(), "Found hourly rates above $500"
    
    def test_job_duration_positive(self):
        """Verify job durations are positive values"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        durations = df['job_duration_days'].dropna()
        assert (durations > 0).all(), "Found non-positive job durations"
    
    def test_is_gap_day_binary(self):
        """Verify is_gap_day flag contains only 0 or 1"""
        df = pd.read_csv(CLEANED_DATA_DIR / 'fact_job_earnings.csv')
        gap_days = df['is_gap_day'].dropna()
        assert gap_days.isin([0, 1]).all(), "is_gap_day contains values other than 0 or 1"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
