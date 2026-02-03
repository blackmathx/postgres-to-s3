import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:

	if df.empty:
		return df
	
	df = df.drop_duplicates(subset="invoice_line_id")
	return df.reset_index(drop=True)