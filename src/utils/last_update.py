from datetime import datetime
from sqlalchemy import text


def fetch_last_update(engine: any, table: str):

    sql = text("""
        SELECT last_ingest 
        FROM ingest_data 
        WHERE table_name = :table
        LIMIT 1
    """)

    last_ingest = "2020-01-01 02:00:00-05"

    with engine.connect() as conn:
        result = conn.execute(sql, {"table": table})
        row = result.fetchone()

        if row:
            last_ingest = row[0]
        else:
            print("No last_ingest date for table", table)
            
            
    return last_ingest



def set_last_update(engine: any, table: str, timestamp: datetime):
    sql = text("""
        UPDATE ingest_data
        SET last_ingest = :timestamp
        WHERE table_name = :table
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, {"timestamp": timestamp, "table": table})
