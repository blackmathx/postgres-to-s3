from datetime import datetime
from sqlalchemy import text


def fetch_last_update(engine: any, table: str):

    sql = text("""
        SELECT last_ingest 
        FROM ingest_data 
        WHERE table_name = :table
            AND success = true
        ORDER BY last_ingest DESC
        LIMIT 1
    """)

    last_ingest = "2020-01-01 01:00:00-05"

    with engine.connect() as conn:
        result = conn.execute(sql, {"table": table})
        row = result.fetchone()

        if row:
            last_ingest = row[0]
            
    return last_ingest



def set_last_update(engine: any, table: str, timestamp: datetime, success: bool):
    sql = text("""
        INSERT INTO ingest_data
        (table_name, last_ingest, success) 
        values
        (:table, :timestamp, :success)
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, {"table": table, "timestamp": timestamp, "success": success})
