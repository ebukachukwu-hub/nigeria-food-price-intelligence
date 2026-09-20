import pandas as pd

file_path = "../data/raw/NGA_RTFP_mkt_2007_2026-08-24.csv"

df = pd.read_csv(file_path)

print("Shape:", df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nFirst 5 rows:")
print(df.head())

print("\nData types:")
print(df.dtypes)

print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicate rows:")
print(df.duplicated().sum())