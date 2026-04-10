"""
Ingests UK trade data from the ONS (Office for National Statistics).

WHY ONS?
- Official UK government statistical body
- Free, public, credible — no scraping, no legal grey areas
- Used by HM Treasury, Bank of England, and UK procurement teams
- The API follows JSON-stat standard used across EU statistical agencies
"""

import requests
import pandas as pd
import os
from datetime import datetime

RAW_DATA_PATH = os.path.join(os.path.dirname(__file__), "../data/raw")

SERIES_TO_FETCH = {
    "BOKI": "uk_imports_total_goods",     # Total UK goods imports (£m)
    "BOKH": "uk_exports_total_goods",     # Total UK goods exports (£m)
    "BQKR": "uk_imports_eu",             # Imports from EU
    "BQKT": "uk_imports_non_eu",         # Imports from non-EU (post-Brexit signal)
    "ELTO": "uk_imports_materials",      # Raw materials
    "ELTQ": "uk_imports_fuels",          # Fuels & energy (cost pressure signal)
    "ELTM": "uk_imports_food",           # Food & beverages
    "ELTR": "uk_imports_manufactured",   # Manufactured goods
}


def fetch_ons_timeseries(series_id: str) -> dict:
    """
    Hits the ONS API for a single time series.

    WHY A SEPARATE FUNCTION PER CONCERN?
    Fetching and parsing are different jobs. Keeping them separate
    means one can test parsing without hitting the API.
    I have structured it in a production pipeline way :).
    """
    url = f"https://api.ons.gov.uk/timeseries/{series_id}/dataset/mret/data"
    print(f"  → Fetching {series_id}...")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  [WARN] {series_id} failed: {e}")
        return {}


def parse_timeseries_to_df(raw_json: dict, series_label: str) -> pd.DataFrame:
    """
    Converts ONS JSON response into a clean DataFrame.

    Since raw API data comes as strings, I'm avoiding
    numeric operations failing silently or throw confusing errors,
    therefore, I'm catching it at the source.... boom!
    """
    months = raw_json.get("months", [])
    if not months:
        return pd.DataFrame()

    records = []
    for entry in months:
        records.append({
            "period":       entry.get("date"),
            "value":        entry.get("value"),
            "series":       series_label,
            "unit":         "GBP_million",
            "source":       "ONS_MRET",
            "ingested_at":  datetime.now().isoformat(),  # audit trail
        })

    df = pd.DataFrame(records)
    df["value"]  = pd.to_numeric(df["value"], errors="coerce")
    df["period"] = pd.to_datetime(df["period"], format="%Y %b", errors="coerce")
    return df


def save_raw(df: pd.DataFrame, filename: str) -> str:
    """
    Save to /data/raw — the untouched source of truth.

    WHY SAVE RAW BEFORE TRANSFORMING?
    If transformation has a bug, just re-run from raw.
    And I do not have to re-hit the API unnecessarily.
    That makes sense in my opinion...
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    filepath = os.path.join(RAW_DATA_PATH, filename)
    df.to_csv(filepath, index=False)
    print(f"[SAVED] {len(df)} rows → {filepath}")
    return filepath


def run_ingestion():
    """
    Make script importable without auto-executing.
    Other scripts or schedulers can call run_ingestion()
    """
    print("=" * 55)
    print("SupplyLens — ONS Trade Data Ingestion")
    print(f"Run started: {datetime.now().isoformat()}")
    print("=" * 55)

    all_frames = []

    for series_id, label in SERIES_TO_FETCH.items():
        raw     = fetch_ons_timeseries(series_id)
        df      = parse_timeseries_to_df(raw, label)
        if not df.empty:
            all_frames.append(df)
            print(f"  ✓ {label}: {len(df)} monthly records")
        else:
            print(f"  ✗ {label}: no data returned")

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        save_raw(combined, "ons_trade_timeseries.csv")
        print(f"\n[DONE] {len(combined)} total records")
        print(f"[DONE] Range: {combined['period'].min()} → {combined['period'].max()}")
        return combined
    else:
        print("[ERROR] No data fetched.")
        return pd.DataFrame()


if __name__ == "__main__":
    df = run_ingestion()
    if not df.empty:
        print("\n--- Sample ---")
        print(df.head(10).to_string(index=False))