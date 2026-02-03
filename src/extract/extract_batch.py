import pandas as pd
from sqlalchemy import create_engine, text




def extract(engine: any, table: str, id_column: str, batch_size: int, last_seen_id: int) -> pd.DataFrame:
    """
    Pull a single batch of rows from `table` where id_column > last_seen_id.
    Returns a DataFrame with up to batch_size rows.
    """
    sql = text(f"""
        SELECT *
        FROM {table}
        WHERE {id_column} > :last_seen_id
        ORDER BY {id_column}
        LIMIT :batch_size
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {"last_seen_id": last_seen_id, "batch_size": batch_size})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()  # empty DataFrame if no data

        df = pd.DataFrame(rows, columns=result.keys())
        return df