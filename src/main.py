from sqlalchemy import create_engine, text

from config.env_reader import EnvReader

from extract import * 
from transform import * 
from load.load import load


def run():
    
    DB_USER = EnvReader.get("DB_USER")
    DB_PASSWORD = EnvReader.get("DB_PASSWORD")
    DB_HOST = EnvReader.get("DB_HOST")
    DB_NAME = EnvReader.get("DB_NAME")
    S3_BUCKET = EnvReader.get("S3_BUCKET")    

    conn_str = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    
    engine = create_engine(conn_str)

    load_date = "load_date=2026-02-02"



 


##### album
    raw_df = extract_album(engine)
    clean_df = gen_transform(raw_df, "album_id")
    s3_key = f"raw/album/{load_date}/album.parquet"
    load(clean_df, S3_BUCKET, s3_key) 
   
##### artist
    raw_df = extract_artist(engine)
    clean_df = transform_artist(raw_df)
    s3_key = f"raw/artist/{load_date}/artist.parquet"
    load(clean_df, S3_BUCKET, s3_key)
    
   

##### customer
    raw_df = extract_customer(engine) 
    clean_df = transform_customer(raw_df)
    s3_key = f"raw/customer/{load_date}/customer.parquet"
    load(clean_df, S3_BUCKET, s3_key)

##### employee
    raw_df = extract_employee(engine) 
    clean_df = transform_employee(raw_df)
    s3_key = f"raw/employee/{load_date}/employee.parquet"
    load(clean_df, S3_BUCKET, s3_key)

##### genre
    raw_df = extract_genre(engine) 
    clean_df = transform_genre(raw_df)
    s3_key = f"raw/genre/{load_date}/genre.parquet"
    load(clean_df, S3_BUCKET, s3_key)

##### invoice
    for year in range(2020, 2028):
        raw_df = extract_invoice(engine, year)
        if raw_df.empty:
            break
        clean_df = transform_invoice(raw_df)
        s3_key = f"raw/invoice/{load_date}/{year}_invoice.parquet"
        load(clean_df, S3_BUCKET, s3_key)
    



##### invoice_line - count 2240
    table_name = "invoice_line"
    id_column = "invoice_line_id"
    batch_size = 1000
    last_seen_id = 0

    while True:
        raw_df = extract_batch(engine, table_name, id_column, batch_size, last_seen_id) 
       
        if raw_df.empty:
            break

        last_seen_id = raw_df[id_column].iloc[-1]
        #print(f"Fetched batch of {len(raw_df)} rows, last_seen_id={last_seen_id}")

        clean_df = transform_invoice_line(raw_df) 
        s3_key = f"raw/invoice_line/{load_date}/part_{last_seen_id}_invoice_line.parquet"
        load(clean_df, S3_BUCKET, s3_key)
        


##### media_type
    raw_df = extract_media_type(engine) 
    clean_df = transform_media_type(raw_df)
    s3_key = f"raw/media_type/{load_date}/media_type.parquet"
    load(clean_df, S3_BUCKET, s3_key)


##### playlist_track - count 8715
    batch_size = 1000
    offset = 0

    while True:
        raw_df = extract_playlist_track(engine, batch_size, offset) 
        if raw_df.empty:
            break
        s3_key = f"raw/playlist_track/{load_date}/part_{offset}_playlist_track.parquet"
        load(raw_df, S3_BUCKET, s3_key)
        offset += batch_size

##### playlist
    raw_df = extract_playlist(engine) 
    clean_df = transform_playlist(raw_df)
    s3_key = f"raw/playlist/{load_date}/playlist.parquet"
    load(clean_df, S3_BUCKET, s3_key)


##### track - count 3503
    table_name = "track"
    id_column = "track_id"
    batch_size = 1000
    last_seen_id = 0
    while True:
        raw_df = extract_batch(engine, table_name, id_column, batch_size, last_seen_id) 
       
        if raw_df.empty:
            break

        last_seen_id = raw_df[id_column].iloc[-1]
        #print(f"Fetched batch of {len(raw_df)} rows, last_seen_id={last_seen_id}")
         
        clean_df = transform_track(raw_df)
        s3_key = f"raw/track/{load_date}/part_{last_seen_id}_track.parquet"
        load(clean_df, S3_BUCKET, s3_key)



    engine.dispose()

if __name__ == '__main__':
    run()