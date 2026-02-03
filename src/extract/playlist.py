import pandas as pd
from sqlalchemy import create_engine, text


def extract(engine: any) -> pd.DataFrame:
	""" 
    Pull raw invoice data from Postgres. Returns a DataFrame.
    """
	SQL_SELECT = text("SELECT * FROM playlist")


	with engine.begin() as conn:
		result = conn.execute(SQL_SELECT)
		df = pd.DataFrame(result.fetchall(), columns=result.keys())

	return df
