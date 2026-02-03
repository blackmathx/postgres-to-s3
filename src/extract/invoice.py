import pandas as pd
from sqlalchemy import create_engine, text


def extract(engine: any, year: int) -> pd.DataFrame:
    """ 
    Pull raw invoice data from Postgres. Returns a DataFrame.
    """
        
    SQL_SELECT_INVOICES = text("SELECT * FROM invoice WHERE invoice_date BETWEEN :start AND :end")
    
 
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    with engine.begin() as conn:
        result = conn.execute(SQL_SELECT_INVOICES, {"start": start_date, "end": end_date})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    return df

    

