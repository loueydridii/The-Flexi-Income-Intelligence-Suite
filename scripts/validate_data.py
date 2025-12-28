"""
Data Validation Script for The Flexi Income Intelligence Suite

This script performs comprehensive data integrity checks on the ETL output.
Run this after executing the ETL pipeline to validate data quality.

Usage:
    python scripts/validate_data.py
"""

import pandas as pd
from pathlib import Path
import sys


class DataValidator:
    """Comprehensive data validation for the data warehouse"""
    
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.errors = []
        self.warnings = []
        
    def load_tables(self):
        """Load all dimension and fact tables"""
        try:
            self.fact = pd.read_csv(self.data_dir / 'fact_job_earnings.csv')
            self.dim_worker = pd.read_csv(self.data_dir / 'dim_worker.csv')
            self.dim_platform = pd.read_csv(self.data_dir / 'dim_platform.csv')
            self.dim_region = pd.read_csv(self.data_dir / 'dim_region.csv')
            self.dim_project = pd.read_csv(self.data_dir / 'dim_project.csv')
            self.dim_date = pd.read_csv(self.data_dir / 'dim_date.csv')
            print("✓ Successfully loaded all tables")
            return True
        except Exception as e:
            self.errors.append(f"Failed to load tables: {str(e)}")
            return False
    
    def validate_primary_keys(self):
        """Validate primary keys have no duplicates or nulls"""
        print("\n=== Validating Primary Keys ===")
        
        pk_checks = {
            'dim_worker': ('worker_id', self.dim_worker),
            'dim_platform': ('platform_id', self.dim_platform),
            'dim_region': ('region_id', self.dim_region),
            'dim_project': ('project_id', self.dim_project),
            'dim_date': ('date_id', self.dim_date),
            'fact_job_earnings': ('job_id', self.fact)
        }
        
        for table_name, (pk_col, df) in pk_checks.items():
            # Check for nulls
            null_count = df[pk_col].isna().sum()
            if null_count > 0:
                self.errors.append(f"{table_name}.{pk_col}: Found {null_count} null values")
            
            # Check for duplicates
            dup_count = df[pk_col].duplicated().sum()
            if dup_count > 0:
                self.errors.append(f"{table_name}.{pk_col}: Found {dup_count} duplicate values")
            
            if null_count == 0 and dup_count == 0:
                print(f"✓ {table_name}.{pk_col}: Valid (no nulls, no duplicates)")
    
    def validate_foreign_keys(self):
        """Validate referential integrity of foreign keys"""
        print("\n=== Validating Foreign Keys ===")
        
        # Worker ID integrity
        fact_workers = set(self.fact['worker_id'].dropna().astype(str))
        dim_workers = set(self.dim_worker['worker_id'].astype(str))
        orphan_workers = fact_workers - dim_workers
        
        if orphan_workers:
            self.errors.append(f"worker_id: {len(orphan_workers)} orphaned records in fact table")
            print(f"✗ worker_id: {len(orphan_workers)} orphaned records")
        else:
            print("✓ worker_id: All references valid")
        
        # Platform ID integrity
        fact_platforms = set(self.fact['platform_id'].dropna().astype(int))
        dim_platforms = set(self.dim_platform['platform_id'].astype(int))
        orphan_platforms = fact_platforms - dim_platforms
        
        if orphan_platforms:
            self.errors.append(f"platform_id: {len(orphan_platforms)} orphaned records in fact table")
            print(f"✗ platform_id: {len(orphan_platforms)} orphaned records")
        else:
            print("✓ platform_id: All references valid")
        
        # Region ID integrity
        fact_regions = set(self.fact['region_id'].dropna().astype(int))
        dim_regions = set(self.dim_region['region_id'].astype(int))
        orphan_regions = fact_regions - dim_regions
        
        if orphan_regions:
            self.errors.append(f"region_id: {len(orphan_regions)} orphaned records in fact table")
            print(f"✗ region_id: {len(orphan_regions)} orphaned records")
        else:
            print("✓ region_id: All references valid")
        
        # Project ID integrity
        fact_projects = set(self.fact['project_id'].dropna().astype(int))
        dim_projects = set(self.dim_project['project_id'].astype(int))
        orphan_projects = fact_projects - dim_projects
        
        if orphan_projects:
            self.errors.append(f"project_id: {len(orphan_projects)} orphaned records in fact table")
            print(f"✗ project_id: {len(orphan_projects)} orphaned records")
        else:
            print("✓ project_id: All references valid")
        
        # Date ID integrity
        fact_dates = set(self.fact['date_id'].dropna().astype(int))
        dim_dates = set(self.dim_date['date_id'].astype(int))
        orphan_dates = fact_dates - dim_dates
        
        if orphan_dates:
            self.errors.append(f"date_id: {len(orphan_dates)} orphaned records in fact table")
            print(f"✗ date_id: {len(orphan_dates)} orphaned records")
        else:
            print("✓ date_id: All references valid")
    
    def validate_data_ranges(self):
        """Validate numeric data is within reasonable ranges"""
        print("\n=== Validating Data Ranges ===")
        
        # Earnings should be non-negative
        negative_earnings = (self.fact['earnings_usd'].dropna() < 0).sum()
        if negative_earnings > 0:
            self.errors.append(f"earnings_usd: Found {negative_earnings} negative values")
            print(f"✗ earnings_usd: {negative_earnings} negative values")
        else:
            print("✓ earnings_usd: All values non-negative")
        
        # Client rating should be 0-5
        ratings = self.fact['client_rating'].dropna()
        invalid_ratings = ((ratings < 0) | (ratings > 5)).sum()
        if invalid_ratings > 0:
            self.errors.append(f"client_rating: Found {invalid_ratings} values outside [0, 5]")
            print(f"✗ client_rating: {invalid_ratings} values outside valid range")
        else:
            print("✓ client_rating: All values in valid range [0, 5]")
        
        # Job success rate should be 0-100
        success_rates = self.fact['job_success_rate'].dropna()
        invalid_rates = ((success_rates < 0) | (success_rates > 100)).sum()
        if invalid_rates > 0:
            self.errors.append(f"job_success_rate: Found {invalid_rates} values outside [0, 100]")
            print(f"✗ job_success_rate: {invalid_rates} values outside valid range")
        else:
            print("✓ job_success_rate: All values in valid range [0, 100]")
        
        # Hourly rate reasonable bounds
        hourly_rates = self.fact['hourly_rate'].dropna()
        low_rates = (hourly_rates < 1).sum()
        high_rates = (hourly_rates > 500).sum()
        
        if low_rates > 0:
            self.warnings.append(f"hourly_rate: Found {low_rates} values below $1")
            print(f"⚠ hourly_rate: {low_rates} values below $1 (unusual)")
        
        if high_rates > 0:
            self.warnings.append(f"hourly_rate: Found {high_rates} values above $500")
            print(f"⚠ hourly_rate: {high_rates} values above $500 (unusual)")
        
        if low_rates == 0 and high_rates == 0:
            print("✓ hourly_rate: All values in reasonable range")
    
    def validate_completeness(self):
        """Check for missing data in critical fields"""
        print("\n=== Validating Data Completeness ===")
        
        critical_fields = ['job_id', 'worker_id', 'date_id']
        
        for field in critical_fields:
            null_count = self.fact[field].isna().sum()
            if null_count > 0:
                self.errors.append(f"{field}: Found {null_count} null values in critical field")
                print(f"✗ {field}: {null_count} null values")
            else:
                print(f"✓ {field}: No missing values")
        
        # Check optional fields and report missing percentage
        optional_fields = ['earnings_usd', 'job_completed', 'hourly_rate', 
                          'client_rating', 'job_success_rate']
        
        print("\nOptional field completeness:")
        for field in optional_fields:
            null_pct = (self.fact[field].isna().sum() / len(self.fact)) * 100
            print(f"  {field}: {100 - null_pct:.1f}% complete ({int(null_pct)}% missing)")
            
            if null_pct > 50:
                self.warnings.append(f"{field}: More than 50% missing data")
    
    def validate_consistency(self):
        """Check for logical consistency in the data"""
        print("\n=== Validating Data Consistency ===")
        
        # is_gap_day should be binary (0 or 1)
        invalid_gap_days = (~self.fact['is_gap_day'].isin([0, 1])).sum()
        if invalid_gap_days > 0:
            self.errors.append(f"is_gap_day: Found {invalid_gap_days} non-binary values")
            print(f"✗ is_gap_day: {invalid_gap_days} invalid values")
        else:
            print("✓ is_gap_day: All values are binary (0 or 1)")
        
        # Job duration should be positive
        negative_duration = (self.fact['job_duration_days'].dropna() <= 0).sum()
        if negative_duration > 0:
            self.errors.append(f"job_duration_days: Found {negative_duration} non-positive values")
            print(f"✗ job_duration_days: {negative_duration} non-positive values")
        else:
            print("✓ job_duration_days: All values positive")
    
    def generate_report(self):
        """Generate final validation report"""
        print("\n" + "=" * 60)
        print("VALIDATION REPORT")
        print("=" * 60)
        
        print(f"\nTable Record Counts:")
        print(f"  fact_job_earnings: {len(self.fact):,} records")
        print(f"  dim_worker: {len(self.dim_worker):,} records")
        print(f"  dim_platform: {len(self.dim_platform):,} records")
        print(f"  dim_region: {len(self.dim_region):,} records")
        print(f"  dim_project: {len(self.dim_project):,} records")
        print(f"  dim_date: {len(self.dim_date):,} records")
        
        if self.errors:
            print(f"\n❌ ERRORS FOUND: {len(self.errors)}")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        else:
            print("\n✓ NO ERRORS FOUND")
        
        if self.warnings:
            print(f"\n⚠ WARNINGS: {len(self.warnings)}")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")
        else:
            print("\n✓ NO WARNINGS")
        
        print("\n" + "=" * 60)
        
        if self.errors:
            print("VALIDATION FAILED - Please fix errors before using the data")
            return False
        else:
            print("VALIDATION PASSED - Data is ready for analysis")
            return True
    
    def run_all_validations(self):
        """Execute all validation checks"""
        print("Starting Data Validation...\n")
        
        if not self.load_tables():
            print("\n❌ Cannot proceed - Failed to load tables")
            return False
        
        self.validate_primary_keys()
        self.validate_foreign_keys()
        self.validate_data_ranges()
        self.validate_completeness()
        self.validate_consistency()
        
        return self.generate_report()


def main():
    """Main execution function"""
    # Determine data directory
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / 'data_cleaned'
    
    if not data_dir.exists():
        print(f"❌ Error: Data directory not found: {data_dir}")
        print("Please run the ETL pipeline first.")
        sys.exit(1)
    
    # Run validation
    validator = DataValidator(data_dir)
    success = validator.run_all_validations()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
