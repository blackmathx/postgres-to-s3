
import pandas as pd

def transform(df: pd.DataFrame, uuid: str) -> pd.DataFrame:
    """
    Clean and enrich raw invoice data. Returns a transformed DataFrame.
    """
    if df.empty:
        return df
    
    
    # Drop duplicates
    df = df.drop_duplicates(subset=uuid)

    return df.reset_index(drop=True)