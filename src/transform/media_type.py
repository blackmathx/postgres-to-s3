import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:

	if df.empty:
		return df
	
	df = df.drop_duplicates(subset="media_type_id")
	return df.reset_index(drop=True)