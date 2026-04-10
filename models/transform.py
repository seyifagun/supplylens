"""

WHY DUCKDB?
- Zero server setup — runs from a single file (supplylens.duckdb)
- Fully SQL-standard — same queries work on Snowflake, BigQuery, Redshift
- Columnar storage — analytical queries run significantly faster than row-based DBs
- Industry momentum — increasingly used in modern data stacks alongside dbt

WHY A STAR SCHEMA?
- Optimised for analytical queries (GROUP BY, aggregations, time series)
- Industry standard for data warehouses — every major cloud DW uses this pattern
- Separates descriptive context (dimensions) from measurements (facts)
- Makes downstream analysis SQL clean and readable
"""

import duckdb
import pandas as pd
import os
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(__file__)
RAW_PATH     = os.path.join(BASE_DIR, "../data/raw/ons_trade_timeseries.csv")
STAGING_PATH = os.path.join(BASE_DIR, "../data/staging")
MARTS_PATH   = os.path.join(BASE_DIR, "../data/marts")
DB_PATH      = os.path.join(BASE_DIR, "../supplylens.duckdb")

os.makedirs(STAGING_PATH, exist_ok=True)
os.makedirs(MARTS_PATH,   exist_ok=True)


def get_connection():
    """
    Returns a DuckDB connection to our project database file.

    WHY A PERSISTENT FILE (.duckdb) AND NOT IN-MEMORY?
    In-memory databases vanish when the script ends.
    A file-based DB means our models persist — the dashboard
    and analysis scripts can query it without re-running transforms.
    """
    return duckdb.connect(DB_PATH)


def build_staging(con):
    """
    STAGING LAYER — clean and standardise raw data.

    Rules applied here:
    1. Rename columns to snake_case standard
    2. Cast types explicitly
    3. Drop any rows with null values
    4. Add a surrogate key (unique row ID)

    WHY A STAGING LAYER?
    You never transform directly from raw to final.
    Staging = one place where all cleaning rules live.
    If a source changes format, you fix it here — nothing downstream breaks.
    """
    print("\n[STAGING] Building stg_trade...")

    con.execute("""
        CREATE OR REPLACE TABLE stg_trade AS
        SELECT
            -- Surrogate key: unique identifier for every row
            -- WHY? Joins and deduplication require a reliable key
            ROW_NUMBER() OVER (ORDER BY period, series) AS trade_id,

            CAST(period AS DATE)       AS trade_date,
            TRIM(series)               AS series_name,
            CAST(value AS DOUBLE)      AS value_gbp_m,
            TRIM(unit)                 AS unit,
            TRIM(source)               AS source,
            CAST(ingested_at AS VARCHAR) AS ingested_at

        FROM read_csv_auto('""" + RAW_PATH.replace("\\", "/") + """')
        WHERE value IS NOT NULL
          AND period IS NOT NULL
          AND series IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM stg_trade").fetchone()[0]
    print(f"  ✓ stg_trade: {count} rows")

    # Export to CSV for transparency and portability
    con.execute(f"""
        COPY stg_trade TO '{(STAGING_PATH + '/stg_trade.csv').replace(chr(92), '/')}'
        (HEADER, DELIMITER ',')
    """)
    print(f"  ✓ Exported → data/staging/stg_trade.csv")


def build_dim_date(con):
    """
    DIMENSION: dim_date — the time dimension.

    WHY A DATE DIMENSION?
    In analytics, you almost always slice data by time — by month,
    quarter, year, or whether it was pre/post a specific event.
    A date dimension pre-computes all these attributes so your
    analysis queries stay clean and don't repeat date logic.

    This is standard practice in every data warehouse.
    """
    print("\n[MARTS] Building dim_date...")

    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            trade_date                                      AS date_id,
            YEAR(trade_date)                                AS year,
            MONTH(trade_date)                               AS month_num,
            STRFTIME(trade_date, '%b')                      AS month_name,
            QUARTER(trade_date)                             AS quarter,
            CONCAT('Q', QUARTER(trade_date), ' ',
                   YEAR(trade_date))                        AS quarter_label,

            -- Period classification — crucial for our supply chain analysis
            -- WHY? We want to compare pre-Brexit vs post-Brexit performance
            CASE
                WHEN trade_date < '2020-03-01' THEN 'pre_covid'
                WHEN trade_date < '2021-01-01' THEN 'covid_disruption'
                WHEN trade_date < '2021-07-01' THEN 'brexit_transition'
                WHEN trade_date < '2022-06-01' THEN 'post_brexit_adjustment'
                WHEN trade_date < '2023-01-01' THEN 'energy_crisis'
                ELSE                                 'stabilisation'
            END AS period_label

        FROM stg_trade
        ORDER BY date_id
    """)

    count = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    print(f"  ✓ dim_date: {count} rows")

    con.execute(f"""
        COPY dim_date TO '{(MARTS_PATH + '/dim_date.csv').replace(chr(92), '/')}'
        (HEADER, DELIMITER ',')
    """)


def build_dim_series(con):
    """
    DIMENSION: dim_series — metadata about each trade series.

    WHY SEPARATE METADATA FROM MEASUREMENTS?
    The series name, category, and trade direction are descriptive attributes —
    they don't change per month. Storing them once in a dimension and
    joining when needed is cleaner and more efficient than repeating
    them in every row of the fact table.
    """
    print("\n[MARTS] Building dim_series...")

    con.execute("""
        CREATE OR REPLACE TABLE dim_series AS
        SELECT
            series_name                             AS series_id,

            -- Human-readable label for dashboard display
            CASE series_name
                WHEN 'uk_imports_total_goods'    THEN 'Total Goods Imports'
                WHEN 'uk_exports_total_goods'    THEN 'Total Goods Exports'
                WHEN 'uk_imports_eu'             THEN 'Imports from EU'
                WHEN 'uk_imports_non_eu'         THEN 'Imports from Non-EU'
                WHEN 'uk_imports_fuels'          THEN 'Fuel & Energy Imports'
                WHEN 'uk_imports_food'           THEN 'Food & Beverage Imports'
                WHEN 'uk_imports_materials'      THEN 'Raw Materials Imports'
                WHEN 'uk_imports_manufactured'   THEN 'Manufactured Goods Imports'
            END AS series_label,

            -- Trade direction — are we looking at imports or exports?
            CASE
                WHEN series_name LIKE '%imports%' THEN 'import'
                WHEN series_name LIKE '%exports%' THEN 'export'
            END AS trade_direction,

            -- Commodity category — groups related series
            CASE series_name
                WHEN 'uk_imports_total_goods'    THEN 'headline'
                WHEN 'uk_exports_total_goods'    THEN 'headline'
                WHEN 'uk_imports_eu'             THEN 'geographic'
                WHEN 'uk_imports_non_eu'         THEN 'geographic'
                WHEN 'uk_imports_fuels'          THEN 'energy'
                WHEN 'uk_imports_food'           THEN 'food'
                WHEN 'uk_imports_materials'      THEN 'materials'
                WHEN 'uk_imports_manufactured'   THEN 'manufactured'
            END AS commodity_category,

            -- Risk weight — how sensitive is this series to supply disruption?
            -- WHY? This feeds directly into our risk scoring model in Phase 4
            -- Fuels and food score highest — most volatile and politically sensitive
            CASE series_name
                WHEN 'uk_imports_fuels'        THEN 0.30
                WHEN 'uk_imports_food'         THEN 0.25
                WHEN 'uk_imports_materials'    THEN 0.20
                WHEN 'uk_imports_manufactured' THEN 0.15
                WHEN 'uk_imports_eu'           THEN 0.05
                WHEN 'uk_imports_non_eu'       THEN 0.05
                ELSE                                0.00
            END AS disruption_risk_weight

        FROM (SELECT DISTINCT series_name FROM stg_trade)
        ORDER BY series_id
    """)

    count = con.execute("SELECT COUNT(*) FROM dim_series").fetchone()[0]
    print(f"  ✓ dim_series: {count} rows")

    con.execute(f"""
        COPY dim_series TO '{(MARTS_PATH + '/dim_series.csv').replace(chr(92), '/')}'
        (HEADER, DELIMITER ',')
    """)


def build_dim_event(con):
    """
    DIMENSION: dim_event — major UK economic disruption events.

    WHY A SEPARATE EVENT DIMENSION?
    This is what makes our project analytically interesting.
    We're not just showing trade values — we're contextualising them
    against real events. This is the kind of thinking that separates
    an Analytics Engineer from a report builder.

    In interviews: 'I built an event dimension so analysts could
    overlay disruption context onto any time series query.'
    """
    print("\n[MARTS] Building dim_event...")

    events = [
        ("EVT001", "COVID-19 Onset",         "2020-03-01", "2020-09-01",
         "Global pandemic causes sharp drop in UK trade volumes across all categories."),
        ("EVT002", "Brexit Transition End",   "2021-01-01", "2021-06-01",
         "UK formally leaves EU single market. EU imports drop ~20%, non-EU diversification begins."),
        ("EVT003", "Supply Chain Crunch",     "2021-06-01", "2022-01-01",
         "Global container shortages and port congestion drive materials and manufactured goods disruption."),
        ("EVT004", "Energy Crisis",           "2022-06-01", "2023-01-01",
         "Russia-Ukraine war triggers UK fuel import cost spike of ~180% by value."),
        ("EVT005", "Food Inflation Peak",     "2022-09-01", "2023-06-01",
         "UK food import costs peak. Border friction compounds supply pressure on perishables."),
        ("EVT006", "Post-Crisis Stabilisation","2023-01-01","2024-12-01",
         "Trade volumes stabilise. Non-EU supplier diversification becomes structural shift."),
    ]

    df_events = pd.DataFrame(events, columns=[
        "event_id", "event_name", "start_date",
        "end_date", "description"
    ])

    con.execute("DROP TABLE IF EXISTS dim_event")
    con.execute("""
        CREATE TABLE dim_event (
            event_id    VARCHAR PRIMARY KEY,
            event_name  VARCHAR,
            start_date  DATE,
            end_date    DATE,
            description VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO dim_event VALUES (?, ?, ?, ?, ?)",
        df_events.values.tolist()
    )

    count = con.execute("SELECT COUNT(*) FROM dim_event").fetchone()[0]
    print(f"  ✓ dim_event: {count} rows")

    con.execute(f"""
        COPY dim_event TO '{(MARTS_PATH + '/dim_event.csv').replace(chr(92), '/')}'
        (HEADER, DELIMITER ',')
    """)


def build_fact_trade_monthly(con):
    """
    FACT TABLE: fact_trade_monthly — the core measurement table.

    This is where everything comes together. The fact table stores:
    - One row per series per month (the grain)
    - Foreign keys to all dimension tables
    - The raw value PLUS derived metrics

    WHY COMPUTE METRICS IN THE FACT TABLE?
    Month-on-month change and rolling averages are expensive to compute
    on the fly in every query. Pre-computing them here means the
    dashboard and analysis layer just SELECT — no complex window
    functions needed downstream.

    This is the Analytics Engineer value-add — making data
    easy to consume for analysts and BI tools.
    """
    print("\n[MARTS] Building fact_trade_monthly...")

    con.execute("""
        CREATE OR REPLACE TABLE fact_trade_monthly AS
        SELECT
            -- Keys
            s.trade_id,
            s.trade_date                                        AS date_id,
            s.series_name                                       AS series_id,

            -- Core measurement
            s.value_gbp_m,

            -- Month-on-month change (%)
            -- WHY? Raw values tell you what happened.
            -- % change tells you how fast things are moving — that's the signal.
            ROUND(
                (s.value_gbp_m - LAG(s.value_gbp_m)
                    OVER (PARTITION BY s.series_name ORDER BY s.trade_date))
                / NULLIF(LAG(s.value_gbp_m)
                    OVER (PARTITION BY s.series_name ORDER BY s.trade_date), 0)
                * 100,
            2) AS mom_change_pct,

            -- 3-month rolling average
            -- WHY? Smooths out noise so you see the trend, not the jitter.
            -- Standard technique in time series analysis.
            ROUND(AVG(s.value_gbp_m)
                OVER (PARTITION BY s.series_name
                      ORDER BY s.trade_date
                      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
            2) AS rolling_3m_avg,

            -- 12-month rolling average (trend baseline)
            ROUND(AVG(s.value_gbp_m)
                OVER (PARTITION BY s.series_name
                      ORDER BY s.trade_date
                      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
            2) AS rolling_12m_avg,

            -- Deviation from 12m average (%)
            -- WHY? This is our disruption signal.
            -- If current value deviates >15% from 12m average, something is happening.
            ROUND(
                (s.value_gbp_m - AVG(s.value_gbp_m)
                    OVER (PARTITION BY s.series_name
                          ORDER BY s.trade_date
                          ROWS BETWEEN 11 PRECEDING AND CURRENT ROW))
                / NULLIF(AVG(s.value_gbp_m)
                    OVER (PARTITION BY s.series_name
                          ORDER BY s.trade_date
                          ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 0)
                * 100,
            2) AS deviation_from_12m_avg_pct,

            -- Metadata
            d.period_label,
            ds.commodity_category,
            ds.trade_direction,
            ds.disruption_risk_weight

        FROM stg_trade s
        LEFT JOIN dim_date   d  ON s.trade_date   = d.date_id
        LEFT JOIN dim_series ds ON s.series_name  = ds.series_id
        ORDER BY s.series_name, s.trade_date
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_trade_monthly").fetchone()[0]
    print(f"  ✓ fact_trade_monthly: {count} rows")

    con.execute(f"""
        COPY fact_trade_monthly TO '{(MARTS_PATH + '/fact_trade_monthly.csv').replace(chr(92), '/')}'
        (HEADER, DELIMITER ',')
    """)


def run_quality_checks(con):
    """
    Basic data quality checks after building the model.

    WHY RUN CHECKS AFTER TRANSFORMS?
    Because transforms can introduce bugs — joins can fan out rows,
    window functions can produce nulls at boundaries, etc.
    Catching this here means you never serve bad data downstream.
    In production this becomes a dbt test suite or Great Expectations.
    """
    print("\n[QA] Running data quality checks...")

    checks = {
        "No null values in fact table": """
            SELECT COUNT(*) FROM fact_trade_monthly
            WHERE value_gbp_m IS NULL
        """,
        "All series have 72 months": """
            SELECT COUNT(*) FROM (
                SELECT series_id, COUNT(*) AS cnt
                FROM fact_trade_monthly
                GROUP BY series_id
                HAVING cnt != 72
            )
        """,
        "Fact row count (expect 576)": """
            SELECT COUNT(*) FROM fact_trade_monthly
        """,
    }

    all_passed = True
    for check_name, query in checks.items():
        result = con.execute(query).fetchone()[0]
        if "expect" in check_name:
            status = "✓" if result == 576 else "✗"
        else:
            status = "✓" if result == 0 else "✗"
            if result != 0:
                all_passed = False
        print(f"  {status} {check_name}: {result}")

    return all_passed


def run_transforms():
    print("=" * 55)
    print("SupplyLens — Data Modelling (Star Schema)")
    print(f"Run started: {datetime.now().isoformat()}")
    print("=" * 55)

    con = get_connection()

    build_staging(con)
    build_dim_date(con)
    build_dim_series(con)
    build_dim_event(con)
    build_fact_trade_monthly(con)
    passed = run_quality_checks(con)

    con.close()

    print("\n" + "=" * 55)
    if passed:
        print("✅ All models built successfully.")
        print(f"   Database → supplylens.duckdb")
        print(f"   Marts    → data/marts/")
    else:
        print("⚠️  Models built with warnings — review QA output above.")
    print("=" * 55)


if __name__ == "__main__":
    run_transforms()