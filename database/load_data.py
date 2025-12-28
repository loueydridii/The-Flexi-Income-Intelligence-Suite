"""
Load Data into SQLite Database

This script loads data from CSV files into the SQLite database.
It creates the database schema and populates all dimension and fact tables.

Usage:
    python database/load_data.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime


class DatabaseLoader:
    """Handles loading CSV data into SQLite database"""
    
    def __init__(self, db_path, data_dir):
        self.db_path = Path(db_path)
        self.data_dir = Path(data_dir)
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Enable foreign key constraints
            self.cursor.execute("PRAGMA foreign_keys = ON")
            
            print(f"✓ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            return False
    
    def create_schema(self):
        """Execute schema.sql to create tables"""
        schema_file = Path(__file__).parent / 'schema.sql'
        
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema creation
            self.cursor.executescript(schema_sql)
            self.conn.commit()
            
            print("✓ Database schema created successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to create schema: {e}")
            return False
    
    def load_dimension_table(self, table_name, csv_file, primary_key):
        """Load a dimension table from CSV"""
        try:
            csv_path = self.data_dir / csv_file
            df = pd.read_csv(csv_path)
            
            # Remove timestamp columns if they exist (we use DB defaults)
            timestamp_cols = ['created_at', 'updated_at']
            df = df.drop(columns=[col for col in timestamp_cols if col in df.columns], errors='ignore')
            
            # Load data
            df.to_sql(table_name, self.conn, if_exists='append', index=False)
            
            record_count = len(df)
            print(f"✓ Loaded {table_name}: {record_count:,} records")
            return record_count
        except Exception as e:
            print(f"✗ Failed to load {table_name}: {e}")
            return 0
    
    def load_fact_table(self, table_name, csv_file):
        """Load the fact table from CSV"""
        try:
            csv_path = self.data_dir / csv_file
            df = pd.read_csv(csv_path)
            
            # Remove timestamp column if it exists
            if 'created_at' in df.columns:
                df = df.drop(columns=['created_at'])
            
            # Handle NULL values for foreign keys (convert pd.NA to None)
            fk_columns = ['platform_id', 'region_id', 'project_id']
            for col in fk_columns:
                if col in df.columns:
                    df[col] = df[col].where(pd.notna(df[col]), None)
            
            # Load data in batches for better performance
            batch_size = 1000
            total_records = len(df)
            
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i+batch_size]
                batch.to_sql(table_name, self.conn, if_exists='append', index=False)
                
                if (i + batch_size) % 5000 == 0:
                    print(f"  ... loaded {min(i + batch_size, total_records):,} / {total_records:,} records")
            
            self.conn.commit()
            print(f"✓ Loaded {table_name}: {total_records:,} records")
            return total_records
        except Exception as e:
            print(f"✗ Failed to load {table_name}: {e}")
            self.conn.rollback()
            return 0
    
    def verify_data_integrity(self):
        """Verify foreign key relationships and data integrity"""
        print("\n=== Verifying Data Integrity ===")
        
        try:
            # Check dimension table counts
            dimensions = [
                ('dim_worker', 'worker_id'),
                ('dim_platform', 'platform_id'),
                ('dim_region', 'region_id'),
                ('dim_project', 'project_id'),
                ('dim_date', 'date_id')
            ]
            
            for table, pk in dimensions:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                print(f"  {table}: {count:,} records")
            
            # Check fact table
            self.cursor.execute("SELECT COUNT(*) FROM fact_job_earnings")
            fact_count = self.cursor.fetchone()[0]
            print(f"  fact_job_earnings: {fact_count:,} records")
            
            # Verify foreign key integrity
            print("\n=== Checking Foreign Key Integrity ===")
            
            # Check for orphaned worker_ids
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM fact_job_earnings f
                LEFT JOIN dim_worker w ON f.worker_id = w.worker_id
                WHERE w.worker_id IS NULL
            """)
            orphan_workers = self.cursor.fetchone()[0]
            
            if orphan_workers == 0:
                print("✓ worker_id: All references valid")
            else:
                print(f"✗ worker_id: {orphan_workers} orphaned records")
            
            # Check for orphaned platform_ids
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM fact_job_earnings f
                LEFT JOIN dim_platform p ON f.platform_id = p.platform_id
                WHERE f.platform_id IS NOT NULL AND p.platform_id IS NULL
            """)
            orphan_platforms = self.cursor.fetchone()[0]
            
            if orphan_platforms == 0:
                print("✓ platform_id: All references valid")
            else:
                print(f"✗ platform_id: {orphan_platforms} orphaned records")
            
            # Check for orphaned date_ids
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM fact_job_earnings f
                LEFT JOIN dim_date d ON f.date_id = d.date_id
                WHERE d.date_id IS NULL
            """)
            orphan_dates = self.cursor.fetchone()[0]
            
            if orphan_dates == 0:
                print("✓ date_id: All references valid")
            else:
                print(f"✗ date_id: {orphan_dates} orphaned records")
            
            return orphan_workers == 0 and orphan_platforms == 0 and orphan_dates == 0
            
        except Exception as e:
            print(f"✗ Integrity check failed: {e}")
            return False
    
    def create_indexes_and_analyze(self):
        """Create indexes and run ANALYZE for query optimization"""
        try:
            print("\n=== Optimizing Database ===")
            
            # Run ANALYZE to update query planner statistics
            self.cursor.execute("ANALYZE")
            self.conn.commit()
            
            print("✓ Database optimization complete")
            return True
        except Exception as e:
            print(f"✗ Optimization failed: {e}")
            return False
    
    def log_etl_run(self, status, records_processed, duration, error_msg=None):
        """Log ETL run metadata"""
        try:
            self.cursor.execute("""
                INSERT INTO etl_metadata (status, records_processed, run_duration_seconds, error_message)
                VALUES (?, ?, ?, ?)
            """, (status, records_processed, duration, error_msg))
            self.conn.commit()
        except Exception as e:
            print(f"Warning: Could not log ETL run: {e}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("\n✓ Database connection closed")
    
    def load_all_data(self):
        """Execute complete data loading process"""
        start_time = datetime.now()
        total_records = 0
        
        try:
            print("\n" + "=" * 60)
            print("LOADING DATA INTO SQLITE DATABASE")
            print("=" * 60 + "\n")
            
            # Create schema
            if not self.create_schema():
                return False
            
            print("\n=== Loading Dimension Tables ===")
            
            # Load dimensions
            total_records += self.load_dimension_table('dim_worker', 'dim_worker.csv', 'worker_id')
            total_records += self.load_dimension_table('dim_platform', 'dim_platform.csv', 'platform_id')
            total_records += self.load_dimension_table('dim_region', 'dim_region.csv', 'region_id')
            total_records += self.load_dimension_table('dim_project', 'dim_project.csv', 'project_id')
            total_records += self.load_dimension_table('dim_date', 'dim_date.csv', 'date_id')
            
            print("\n=== Loading Fact Table ===")
            
            # Load fact table
            total_records += self.load_fact_table('fact_job_earnings', 'fact_job_earnings.csv')
            
            # Verify integrity
            integrity_ok = self.verify_data_integrity()
            
            # Optimize
            self.create_indexes_and_analyze()
            
            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Log ETL run
            status = 'SUCCESS' if integrity_ok else 'FAILED'
            self.log_etl_run(status, total_records, duration)
            
            print("\n" + "=" * 60)
            print("DATA LOAD COMPLETE")
            print("=" * 60)
            print(f"Total records loaded: {total_records:,}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Status: {status}")
            print("=" * 60 + "\n")
            
            return integrity_ok
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = str(e)
            
            self.log_etl_run('FAILED', total_records, duration, error_msg)
            
            print(f"\n✗ Data load failed: {e}")
            return False


def main():
    """Main execution function"""
    # Determine paths
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / 'database' / 'freelance_earnings.db'
    data_dir = base_dir / 'data_cleaned'
    
    # Check if data directory exists
    if not data_dir.exists():
        print(f"✗ Error: Data directory not found: {data_dir}")
        print("Please run the ETL pipeline first to generate cleaned data.")
        sys.exit(1)
    
    # Create database directory if it doesn't exist
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize loader
    loader = DatabaseLoader(db_path, data_dir)
    
    # Connect to database
    if not loader.connect():
        sys.exit(1)
    
    try:
        # Load all data
        success = loader.load_all_data()
        
        if success:
            print(f"✓ Database ready at: {db_path}")
            print("\nYou can now query the database using:")
            print(f"  sqlite3 {db_path}")
            print("\nOr connect from Python:")
            print(f"  import sqlite3")
            print(f"  conn = sqlite3.connect('{db_path}')")
            sys.exit(0)
        else:
            print("\n✗ Data load completed with errors")
            sys.exit(1)
            
    finally:
        loader.close()


if __name__ == '__main__':
    main()
