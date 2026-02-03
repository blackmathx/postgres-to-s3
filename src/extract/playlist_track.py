import pandas as pd
from sqlalchemy import create_engine, text


def extract(engine: any, batch_size: int, offset: int) -> pd.DataFrame:
    """ 
    Pull raw playlist_track data from Postgres. Returns a DataFrame.
    """
    sql = text(f"""
        SELECT *
        FROM playlist_track
        LIMIT :batch_size OFFSET :offset
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {"batch_size": batch_size, "offset": offset})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()  # empty DataFrame if no data

        df = pd.DataFrame(rows, columns=result.keys())
        return df