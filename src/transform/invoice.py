

import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich raw invoice data. Returns a transformed DataFrame.
    """

    if df.empty:
        return df
    
    # Drop duplicates
    df = df.drop_duplicates(subset="invoice_id")

    # Parse dates
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])
    # df["created_at"] = pd.to_datetime(df["created_at"])

    # Drop rows missing critical fields
    df = df.dropna(subset=["invoice_id", "total"])

    # Ensure amount is numeric and non-negative
    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df = df[df["total"] >= 0]

    # Normalize status to lowercase
    # df["status"] = df["status"].str.strip().str.lower()

    # Derive new columns
    # df["invoice_year"] = df["invoice_date"].dt.year
    # df["invoice_month"] = df["invoice_date"].dt.month

    return df.reset_index(drop=True)