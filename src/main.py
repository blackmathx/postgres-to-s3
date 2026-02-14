from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import uuid 

from config.env_reader import EnvReader
from extract import * 
from transform import * 
from load.load import load
from utils import set_last_update 
from utils import fetch_last_update



def run():
    
    DB_USER = EnvReader.get("DB_USER")
    DB_PASSWORD = EnvReader.get("DB_PASSWORD")
    DB_HOST = EnvReader.get("DB_HOST")
    DB_NAME = EnvReader.get("DB_NAME")
    S3_BUCKET = EnvReader.get("S3_BUCKET")    

    conn_str = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    
    engine = create_engine(conn_str)

    load_date = datetime.now().strftime("%Y-%m-%d")

    batch_size = 1000
    
    tables = ["album", "artist", "customer", "employee", "genre", "invoice", "invoice_line", 
              "media_type", "playlist", "track"]
    
    # bridge tables do not have a pk id column
    tables_no_id = ["playlist_track"]




    for table in tables:
        updated_at = datetime.now(timezone.utc)
        last_ingest = fetch_last_update(engine, table)
        id_column = table + "_id"
        last_seen_id = 0

        while True:
            raw_df = extract_batch(engine, table, id_column, batch_size, last_seen_id, last_ingest) 
        
            if raw_df.empty:
                break

            last_seen_id = raw_df[id_column].iloc[-1]
            clean_df = gen_transform(raw_df, table + "_id") 

            ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            rand = uuid.uuid4().hex[:8]
            file_id = f"{ts}_{rand}"
            
            s3_key = f"raw/{load_date}/{table}/{table}_{file_id}.parquet"
            load(clean_df, S3_BUCKET, s3_key)
        set_last_update(engine, table, updated_at, True)
    


    for table in tables_no_id:
        # batch with limit offset for bridge tables

        updated_at = datetime.now(timezone.utc)
        last_ingest = fetch_last_update(engine, table)

        order_by_column = "playlist_id"
        offset = 0


        while True: 
            raw_df = extract_batch_woffset(engine, table, last_ingest, order_by_column, batch_size, offset)

            if raw_df.empty:
                break

            ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            rand = uuid.uuid4().hex[:8]
            file_id = f"{ts}_{rand}"


            s3_key = f"raw/{load_date}/{table}/{table}_{file_id}.parquet"
            load(raw_df, S3_BUCKET, s3_key)
            offset += batch_size 
        set_last_update(engine, table, updated_at, True)
    
    engine.dispose()



if __name__ == '__main__':
    run()