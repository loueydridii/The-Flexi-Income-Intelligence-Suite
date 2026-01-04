-- ============================================================================
-- The Flexi Income Intelligence Suite - Database Schema (SQLite)
-- ============================================================================
-- Star Schema for Freelance Job Earnings Data Warehouse
-- Created: December 28, 2025
-- ============================================================================

-- Drop existing tables if they exist (for clean recreation)
DROP TABLE IF EXISTS fact_job_earnings;
DROP TABLE IF EXISTS dim_worker;
DROP TABLE IF EXISTS dim_platform;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_project;
DROP TABLE IF EXISTS dim_date;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_worker: Worker/Freelancer dimension
-- ----------------------------------------------------------------------------
CREATE TABLE dim_worker (
    worker_id TEXT PRIMARY KEY NOT NULL,
    experience_level TEXT,
    primary_skill TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_worker_experience ON dim_worker(experience_level);
CREATE INDEX idx_worker_skill ON dim_worker(primary_skill);

-- ----------------------------------------------------------------------------
-- dim_platform: Freelance platform dimension
-- ----------------------------------------------------------------------------
CREATE TABLE dim_platform (
    platform_id INTEGER PRIMARY KEY NOT NULL,
    platform_name TEXT NOT NULL,
    category TEXT,
    payment_cycle TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform_name)
);

CREATE INDEX idx_platform_name ON dim_platform(platform_name);
CREATE INDEX idx_platform_category ON dim_platform(category);

-- ----------------------------------------------------------------------------
-- dim_region: Geographic region dimension
-- ----------------------------------------------------------------------------
CREATE TABLE dim_region (
    region_id INTEGER PRIMARY KEY NOT NULL,
    region TEXT NOT NULL,
    cost_of_living_index REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region)
);

CREATE INDEX idx_region_name ON dim_region(region);

-- ----------------------------------------------------------------------------
-- dim_project: Project type dimension
-- ----------------------------------------------------------------------------
CREATE TABLE dim_project (
    project_id INTEGER PRIMARY KEY NOT NULL,
    project_type TEXT NOT NULL,
    job_category TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_type, job_category)
);

CREATE INDEX idx_project_type ON dim_project(project_type);
CREATE INDEX idx_project_category ON dim_project(job_category);

-- ----------------------------------------------------------------------------
-- dim_date: Date dimension for time-based analysis
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY NOT NULL,
    full_date DATE NOT NULL UNIQUE,
    day_of_week TEXT NOT NULL,
    is_weekend INTEGER NOT NULL CHECK(is_weekend IN (0, 1)),
    is_holiday INTEGER NOT NULL CHECK(is_holiday IN (0, 1)),
    month_name TEXT NOT NULL,
    month_number INTEGER NOT NULL CHECK(month_number BETWEEN 1 AND 12),
    quarter INTEGER NOT NULL CHECK(quarter BETWEEN 1 AND 4),
    year INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_date_full ON dim_date(full_date);
CREATE INDEX idx_date_year_month ON dim_date(year, month_number);
CREATE INDEX idx_date_quarter ON dim_date(year, quarter);
CREATE INDEX idx_date_weekend ON dim_date(is_weekend);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_job_earnings: Main fact table for job earnings and metrics
-- ----------------------------------------------------------------------------
CREATE TABLE fact_job_earnings (
    job_id INTEGER PRIMARY KEY NOT NULL,
    worker_id TEXT NOT NULL,
    platform_id INTEGER,
    region_id INTEGER,
    project_id INTEGER,
    date_id INTEGER NOT NULL,
    
    -- Metrics
    earnings_usd REAL CHECK(earnings_usd >= 0),
    job_completed INTEGER,
    job_duration_days INTEGER CHECK(job_duration_days > 0),
    hourly_rate REAL CHECK(hourly_rate >= 0),
    job_success_rate REAL CHECK(job_success_rate BETWEEN 0 AND 100),
    client_rating REAL CHECK(client_rating BETWEEN 0 AND 5),
    rehire_rate REAL CHECK(rehire_rate BETWEEN 0 AND 100),
    marketing_spend REAL CHECK(marketing_spend >= 0),
    is_gap_day INTEGER NOT NULL CHECK(is_gap_day IN (0, 1)),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints
    FOREIGN KEY (worker_id) REFERENCES dim_worker(worker_id),
    FOREIGN KEY (platform_id) REFERENCES dim_platform(platform_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    FOREIGN KEY (project_id) REFERENCES dim_project(project_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- Indexes for fact table (optimized for common query patterns)
CREATE INDEX idx_fact_worker ON fact_job_earnings(worker_id);
CREATE INDEX idx_fact_platform ON fact_job_earnings(platform_id);
CREATE INDEX idx_fact_region ON fact_job_earnings(region_id);
CREATE INDEX idx_fact_project ON fact_job_earnings(project_id);
CREATE INDEX idx_fact_date ON fact_job_earnings(date_id);
CREATE INDEX idx_fact_earnings ON fact_job_earnings(earnings_usd);
CREATE INDEX idx_fact_gap_day ON fact_job_earnings(is_gap_day);

-- Composite indexes for common query patterns
CREATE INDEX idx_fact_platform_date ON fact_job_earnings(platform_id, date_id);
CREATE INDEX idx_fact_region_date ON fact_job_earnings(region_id, date_id);
CREATE INDEX idx_fact_worker_date ON fact_job_earnings(worker_id, date_id);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- vw_job_earnings_detailed: Complete fact table with all dimension attributes
-- ----------------------------------------------------------------------------
CREATE VIEW vw_job_earnings_detailed AS
SELECT 
    f.job_id,
    f.date_id,
    d.full_date,
    d.year,
    d.quarter,
    d.month_name,
    d.day_of_week,
    d.is_weekend,
    
    w.worker_id,
    w.experience_level,
    w.primary_skill,
    
    p.platform_id,
    p.platform_name,
    p.category AS platform_category,
    p.payment_cycle,
    
    r.region_id,
    r.region,
    r.cost_of_living_index,
    
    pr.project_id,
    pr.project_type,
    pr.job_category,
    
    f.earnings_usd,
    f.job_completed,
    f.job_duration_days,
    f.hourly_rate,
    f.job_success_rate,
    f.client_rating,
    f.rehire_rate,
    f.marketing_spend,
    f.is_gap_day
FROM fact_job_earnings f
LEFT JOIN dim_worker w ON f.worker_id = w.worker_id
LEFT JOIN dim_platform p ON f.platform_id = p.platform_id
LEFT JOIN dim_region r ON f.region_id = r.region_id
LEFT JOIN dim_project pr ON f.project_id = pr.project_id
LEFT JOIN dim_date d ON f.date_id = d.date_id;

-- ----------------------------------------------------------------------------
-- vw_platform_summary: Aggregated metrics by platform
-- ----------------------------------------------------------------------------
CREATE VIEW vw_platform_summary AS
SELECT 
    p.platform_name,
    COUNT(*) AS total_jobs,
    SUM(f.earnings_usd) AS total_earnings,
    AVG(f.earnings_usd) AS avg_earnings,
    AVG(f.hourly_rate) AS avg_hourly_rate,
    AVG(f.client_rating) AS avg_client_rating,
    AVG(f.job_success_rate) AS avg_success_rate,
    SUM(f.job_completed) AS total_completed_jobs
FROM fact_job_earnings f
INNER JOIN dim_platform p ON f.platform_id = p.platform_id
GROUP BY p.platform_id, p.platform_name;

-- ----------------------------------------------------------------------------
-- vw_worker_performance: Worker performance metrics
-- ----------------------------------------------------------------------------
CREATE VIEW vw_worker_performance AS
SELECT 
    w.worker_id,
    w.experience_level,
    w.primary_skill,
    COUNT(*) AS total_jobs,
    SUM(f.earnings_usd) AS total_earnings,
    AVG(f.earnings_usd) AS avg_earnings_per_job,
    AVG(f.hourly_rate) AS avg_hourly_rate,
    AVG(f.client_rating) AS avg_client_rating,
    AVG(f.job_success_rate) AS avg_success_rate,
    SUM(f.job_completed) AS total_completed,
    SUM(f.is_gap_day) AS gap_days_count
FROM fact_job_earnings f
INNER JOIN dim_worker w ON f.worker_id = w.worker_id
GROUP BY w.worker_id, w.experience_level, w.primary_skill;

-- ----------------------------------------------------------------------------
-- vw_regional_analysis: Regional earnings and cost of living analysis
-- ----------------------------------------------------------------------------
CREATE VIEW vw_regional_analysis AS
SELECT 
    r.region,
    r.cost_of_living_index,
    COUNT(*) AS total_jobs,
    SUM(f.earnings_usd) AS total_earnings,
    AVG(f.earnings_usd) AS avg_earnings,
    AVG(f.hourly_rate) AS avg_hourly_rate,
    AVG(f.client_rating) AS avg_client_rating,
    CASE 
        WHEN r.cost_of_living_index IS NOT NULL 
        THEN AVG(f.earnings_usd) / r.cost_of_living_index 
        ELSE NULL 
    END AS earnings_vs_col_ratio
FROM fact_job_earnings f
INNER JOIN dim_region r ON f.region_id = r.region_id
GROUP BY r.region_id, r.region, r.cost_of_living_index;

-- ----------------------------------------------------------------------------
-- vw_monthly_trends: Monthly earnings trends
-- ----------------------------------------------------------------------------
CREATE VIEW vw_monthly_trends AS
SELECT 
    d.year,
    d.month_number,
    d.month_name,
    COUNT(*) AS total_jobs,
    SUM(f.earnings_usd) AS total_earnings,
    AVG(f.earnings_usd) AS avg_earnings,
    SUM(f.job_completed) AS total_completed,
    COUNT(DISTINCT f.worker_id) AS active_workers
FROM fact_job_earnings f
INNER JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month_number, d.month_name
ORDER BY d.year, d.month_number;

-- ============================================================================
-- METADATA TABLE (Optional - for tracking ETL runs)
-- ============================================================================

CREATE TABLE etl_metadata (
    etl_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_processed INTEGER,
    status TEXT CHECK(status IN ('SUCCESS', 'FAILED', 'RUNNING')),
    error_message TEXT,
    run_duration_seconds REAL
);

-- ============================================================================
-- DATABASE STATISTICS AND COMMENTS
-- ============================================================================

-- Enable foreign key constraints (important for SQLite)
PRAGMA foreign_keys = ON;

-- Analyze tables for query optimization
ANALYZE;
