import pandas as pd
from sqlalchemy import create_engine, text




def extract(engine: any, table: str, last_ingest: str, order_by: str, batch_size: int, offset: int) -> pd.DataFrame:
    """
    extract batch with offset. for bridge tables without a pk id column. this orders by a give column
    and selects with limit-offset
    """
    sql = text(f"""
        SELECT *
        FROM {table}
        WHERE updated_at > :last_ingest
        ORDER BY {order_by} asc
        LIMIT :batch_size
        OFFSET :offset
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {"last_ingest": last_ingest, "batch_size": batch_size, "offset": offset})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()  # empty DataFrame if no data

        df = pd.DataFrame(rows, columns=result.keys())
        return df