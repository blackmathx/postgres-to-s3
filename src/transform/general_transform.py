
import pandas as pd

def transform(df: pd.DataFrame, uuid: str) -> pd.DataFrame:
    """
    clean data and return the transformed dataframe.
    """
    if df.empty:
        return df
    
    
    # Drop duplicates
    df = df.drop_duplicates(subset=uuid)

    return df.reset_index(drop=True)