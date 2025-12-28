# The Flexi Income Intelligence Suite

A comprehensive **ETL (Extract, Transform, Load) pipeline** and data warehouse solution for analyzing freelance job earnings across multiple gig economy platforms. This project transforms raw, inconsistent freelance transaction data into a clean, analytics-ready star schema optimized for business intelligence and data analysis.

## Project Overview

The Flexi Income Intelligence Suite enables deep analysis of the gig economy by processing transaction data from major freelance platforms (Upwork, Fiverr, Freelancer, Toptal, PeoplePerHour) and organizing it into a dimensional data warehouse.

### Key Features

- **Automated ETL Pipeline**: Jupyter notebook-based pipeline for data extraction, cleaning, and transformation
- **Star Schema Design**: Optimized dimensional model with fact and dimension tables
- **Data Quality Management**: Handles duplicates, missing values, text standardization, and referential integrity
- **Comprehensive Metrics**: Tracks earnings, job completion rates, client ratings, marketing spend, and more
- **Multi-Platform Support**: Aggregates data from 5+ freelance platforms
- **Time Series Ready**: Full date dimension with weekends, holidays, and quarters

### Use Cases

- Analyze freelancer earnings patterns across platforms and regions
- Compare regional cost-of-living vs. earnings
- Track job success rates and client satisfaction metrics
- Evaluate marketing spend effectiveness
- Identify seasonal trends in freelance work
- Measure experience level impact on earnings
- Monitor rehire rates and client relationships

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW DATA SOURCES                         │
├─────────────────────────────────────────────────────────────────┤
│  • jobs_transactions.csv (15,502 records)                       │
│  • workers.json (Worker profiles & skills)                       │
│  • platforms.csv (Platform metadata)                             │
│  • regions.json (Geographic + cost-of-living data)               │
│  • projects.csv (Project types & categories)                     │
│  • dates.csv (Date dimension)                                    │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ETL PIPELINE                                │
│                  (etl/etl_pipeline.ipynb)                        │
├─────────────────────────────────────────────────────────────────┤
│  1. EXTRACT: Load raw CSV/JSON files                            │
│  2. CLEAN:                                                       │
│     • Remove duplicates                                          │
│     • Handle missing values                                      │
│     • Standardize text (fix casing/whitespace)                   │
│     • Validate referential integrity                             │
│  3. TRANSFORM: Build star schema                                 │
│  4. LOAD: Export dimension & fact tables                         │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STAR SCHEMA OUTPUT                             │
│                   (data_cleaned/)                                │
├─────────────────────────────────────────────────────────────────┤
│  FACT TABLE:                                                     │
│  • fact_job_earnings.csv                                         │
│                                                                  │
│  DIMENSION TABLES:                                               │
│  • dim_worker.csv (Experience & skills)                          │
│  • dim_platform.csv (Platform details)                           │
│  • dim_region.csv (Geographic + cost-of-living)                  │
│  • dim_project.csv (Project types & categories)                  │
│  • dim_date.csv (Time dimension)                                 │
└─────────────────────────────────────────────────────────────────┘
                     │
                     ▼
           [Business Intelligence Tools]
           Power BI / Tableau / Python Analytics
```

## Project Structure

```
The-Flexi-Income-Intelligence-Suite/
│
├── data_raw/                      # Raw source data
│   ├── jobs_transactions.csv     # Main transaction records (15,502 rows)
│   ├── workers.json               # Worker profiles with skills
│   ├── platforms.csv              # Platform metadata
│   ├── regions.json               # Regional data
│   ├── projects.csv               # Project classifications
│   └── dates.csv                  # Date dimension seed data
│
├── data_cleaned/                  # Processed star schema output
│   ├── fact_job_earnings.csv     # Fact table (core metrics)
│   ├── dim_worker.csv             # Worker dimension
│   ├── dim_platform.csv           # Platform dimension
│   ├── dim_region.csv             # Region dimension
│   ├── dim_project.csv            # Project dimension
│   └── dim_date.csv               # Date dimension
│
├── etl/                           # ETL pipeline code
│   └── etl_pipeline.ipynb         # Main ETL notebook
│
├── database/                      # SQLite database implementation
│   ├── schema.sql                 # Database DDL (tables, indexes, views)
│   ├── load_data.py               # Data loading script
│   ├── README.md                  # Database documentation & ER diagram
│   └── freelance_earnings.db      # SQLite database (generated)
│
├── tests/                         # Test suite
│   ├── __init__.py                # Package initializer
│   ├── test_data_quality.py       # Data quality tests
│   └── test_etl_functions.py      # ETL unit tests
│
├── scripts/                       # Utility scripts
│   └── validate_data.py           # Data validation script
│
├── data_dictionary_v2.csv         # Complete data dictionary
├── data_dictionary_v2.xlsx        # Data dictionary (Excel format)
├── requirements.txt               # Python dependencies
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Jupyter Notebook or JupyterLab
- Git

### Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/loueydridii/The-Flexi-Income-Intelligence-Suite.git
   cd The-Flexi-Income-Intelligence-Suite
   ```

2. **Create a virtual environment** (recommended)

   ```bash
   # Windows
   python -m venv venv
   venv\Scripts\activate

   # macOS/Linux
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Launch Jupyter Notebook**

   ```bash
   jupyter notebook
   ```

5. **Open and run the ETL pipeline**

   - Navigate to `etl/etl_pipeline.ipynb`
   - Run all cells sequentially (Cell → Run All)
   - Cleaned data will be generated in `data_cleaned/`

6. **Load data into SQLite database** (optional but recommended)
   ```bash
   python database/load_data.py
   ```
   This creates a SQLite database with proper constraints, indexes, and views for efficient querying.

## Usage

### Running the ETL Pipeline

1. Open `etl/etl_pipeline.ipynb` in Jupyter
2. Execute cells sequentially to:
   - Load raw data from `data_raw/`
   - Apply data cleaning transformations
   - Generate star schema tables in `data_cleaned/`
3. Review the output tables for analysis

### Using the Database

After loading data into SQLite:

```bash
# Connect to database
sqlite3 database/freelance_earnings.db

# Example queries
SELECT * FROM vw_platform_summary;
SELECT * FROM vw_worker_performance LIMIT 10;
```

Or from Python:

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('database/freelance_earnings.db')
df = pd.read_sql_query("SELECT * FROM vw_platform_summary", conn)
print(df)
conn.close()
```

See [database/README.md](database/README.md) for complete database documentation, ER diagram, and query examples.

### Data Dictionary

Refer to [data_dictionary_v2.csv](data_dictionary_v2.csv) for complete schema documentation including:

- Table and column definitions
- Data types and constraints
- Primary/foreign key relationships
- Sample values and descriptions

### Key Tables

**Fact Table: `fact_job_earnings.csv`**

- Core business metrics: earnings, job completion, duration, rates
- Foreign keys to all dimension tables
- Includes `is_gap_day` flag for income gap analysis

**Dimension Tables:**

- `dim_worker`: Worker experience levels and primary skills
- `dim_platform`: Platform names, categories, payment cycles
- `dim_region`: Geographic regions with cost-of-living indices
- `dim_project`: Project types (Hourly/Fixed) and job categories
- `dim_date`: Complete date dimension with business calendar attributes

## Testing & Validation

### Running Tests

The project includes comprehensive test coverage for data quality and ETL functions.

**Run all tests:**

```bash
pytest tests/ -v
```

**Run specific test file:**

```bash
# Data quality tests
pytest tests/test_data_quality.py -v

# ETL function tests
pytest tests/test_etl_functions.py -v
```

**Run with coverage report:**

```bash
pytest tests/ --cov --cov-report=html
```

### Data Validation

After running the ETL pipeline, validate data integrity:

```bash
python scripts/validate_data.py
```

This script performs:

- Primary key validation (duplicates, nulls)
- Foreign key referential integrity checks
- Data range validation (earnings, ratings, success rates)
- Completeness checks for critical fields
- Logical consistency validation

**Example output:**

```
=== VALIDATION REPORT ===
Table Record Counts:
  fact_job_earnings: 15,502 records
  dim_worker: 1,000 records
  ...

✓ NO ERRORS FOUND
✓ NO WARNINGS

VALIDATION PASSED - Data is ready for analysis
```

### Test Coverage

**Data Quality Tests** (`test_data_quality.py`):

- Dimension table integrity (no duplicate PKs, no nulls)
- Fact table validation (non-negative earnings, valid rating ranges)
- Referential integrity between all tables
- Data consistency rules (binary flags, positive durations)

**ETL Function Tests** (`test_etl_functions.py`):

- Date ID generation (YYYYMMDD format)
- Text standardization (case-insensitive matching)
- Gap day calculation logic
- Data cleaning operations (duplicates, whitespace)
- Merge operations (case-insensitive joins)

## Sample Analysis Queries

### Using Pandas

```python
import pandas as pd

# Load the data
fact = pd.read_csv('data_cleaned/fact_job_earnings.csv')
dim_platform = pd.read_csv('data_cleaned/dim_platform.csv')
dim_region = pd.read_csv('data_cleaned/dim_region.csv')

# Analyze earnings by platform
platform_earnings = fact.merge(dim_platform, on='platform_id')
avg_by_platform = platform_earnings.groupby('platform_name')['earnings_usd'].mean()
print(avg_by_platform.sort_values(ascending=False))

# Compare earnings vs cost of living by region
region_analysis = fact.merge(dim_region, on='region_id')
regional_metrics = region_analysis.groupby('region').agg({
    'earnings_usd': 'mean',
    'cost_of_living_index': 'first'
})
print(regional_metrics)
```

### Using SQL (after loading to database)

```sql
-- Top 10 highest earning workers
SELECT
    w.worker_id,
    w.experience_level,
    w.primary_skill,
    SUM(f.earnings_usd) as total_earnings
FROM fact_job_earnings f
JOIN dim_worker w ON f.worker_id = w.worker_id
GROUP BY w.worker_id, w.experience_level, w.primary_skill
ORDER BY total_earnings DESC
LIMIT 10;

-- Platform performance comparison
SELECT
    p.platform_name,
    COUNT(*) as job_count,
    AVG(f.earnings_usd) as avg_earnings,
    AVG(f.client_rating) as avg_rating
FROM fact_job_earnings f
JOIN dim_platform p ON f.platform_id = p.platform_id
GROUP BY p.platform_name
ORDER BY avg_earnings DESC;
```

## Data Cleaning Strategies

The ETL pipeline implements several data quality measures:

- **Duplicate Removal**: Identifies and removes duplicate job records
- **Missing Value Handling**: Imputes or filters missing data appropriately
- **Text Standardization**: Fixes inconsistent casing (e.g., "FiVerR" → "Fiverr", "asia" → "Asia")
- **Data Validation**: Ensures referential integrity across all foreign key relationships
- **Outlier Detection**: Flags unusual values for review
- **Gap Day Flagging**: Identifies days with zero earnings or incomplete jobs

## Contributing

Contributions are welcome! Here's how you can help:

### How to Contribute

1. **Fork the repository**

   ```bash
   git fork https://github.com/loueydridii/The-Flexi-Income-Intelligence-Suite.git
   ```

2. **Create a feature branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**

   - Follow existing code style and conventions
   - Add comments for complex logic
   - Update documentation as needed

4. **Test your changes**

   - Run the entire ETL pipeline to ensure no errors
   - Verify data quality in output tables

5. **Commit your changes**

   ```bash
   git add .
   git commit -m "Add: Brief description of your changes"
   ```

6. **Push to your fork**

   ```bash
   git push origin feature/your-feature-name
   ```

7. **Submit a Pull Request**
   - Provide a clear description of your changes
   - Reference any related issues

### Contribution Ideas

- Add visualization dashboards (Power BI, Tableau, Plotly)
- Create additional analysis notebooks
- Implement additional data quality tests
- Migrate to PostgreSQL/MySQL for production use
- Build predictive models for earnings forecasting
- Add support for additional freelance platforms
- Implement Slowly Changing Dimensions (SCD Type 2)
- Create REST API for database access
- Improve documentation and examples
- Fix bugs or optimize performance

### Code Style Guidelines

- Use descriptive variable names
- Add docstrings to functions
- Keep functions focused and modular
- Comment complex transformations
- Follow PEP 8 for Python code

## Data Dictionary Quick Reference

| Table                 | Description                      | Key Metrics                                              |
| --------------------- | -------------------------------- | -------------------------------------------------------- |
| **fact_job_earnings** | Main fact table with job metrics | earnings_usd, job_completed, duration, rates, ratings    |
| **dim_worker**        | Worker profiles                  | worker_id (PK), experience_level, primary_skill          |
| **dim_platform**      | Freelance platforms              | platform_id (PK), platform_name, category, payment_cycle |
| **dim_region**        | Geographic regions               | region_id (PK), region, cost_of_living_index             |
| **dim_project**       | Project classifications          | project_id (PK), project_type, job_category              |
| **dim_date**          | Date dimension                   | date_id (PK), full_date, is_weekend, quarter, year       |

## Future Enhancements

- [ ] Automated data ingestion from APIs
- [ ] Real-time dashboard with Power BI/Tableau
- [ ] Machine learning models for earnings prediction
- [ ] Migrate to PostgreSQL/MySQL for production scale
- [ ] Implement Slowly Changing Dimensions (SCD Type 2) for history tracking
- [ ] Docker containerization
- [ ] REST API for data access
- [ ] CI/CD pipeline for ETL automation
- [ ] Automated email alerts for data quality issues
- [ ] Incremental data loading strategy

## License

This project is available for educational and analytical purposes. Please add an appropriate license file if you plan to distribute or modify this project.

## Authors

- **Original Author**: [loueydridii](https://github.com/loueydridii)

## Contact & Support

- **Issues**: [GitHub Issues](https://github.com/loueydridii/The-Flexi-Income-Intelligence-Suite/issues)
- **Discussions**: [GitHub Discussions](https://github.com/loueydridii/The-Flexi-Income-Intelligence-Suite/discussions)

## Acknowledgments

- Data structure inspired by Kimball dimensional modeling methodology
- Built with pandas, NumPy, and Jupyter ecosystem
- Thanks to the open-source community

---

**If you find this project useful, please consider giving it a star!**

---

_Last Updated: December 28, 2025_
