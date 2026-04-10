"""
Sanity-checked raw data before transforming it as a meaning
of it acting as a proper data quality test suite.
"""

import pandas as pd

df = pd.read_csv("data/raw/ons_trade_timeseries.csv", parse_dates=["period"])

print("Shape:   ", df.shape)
print("Columns:    ", df.columns.tolist())
print("Nulls:\n",     df.isnull().sum())
print("Date range: ", df["period"].min(), "→", df["period"].max())
print("Series:     ", df["series"].unique())
print("\nSample:")
print(df.head(8).to_string(index=False))