## Postgres to S3

Postgres ETL to S3. Batch data from my database instance to S3


Libs: sqlalchemy psycopg[binary] pandas boto3 pyarrow

Test: python -m unittest tests/test.py

#### Load Logs
```
Loaded 347 rows → s3://media-retail-31415/raw/album/load_date=2026-02-02/album.parquet
Loaded 275 rows → s3://media-retail-31415/raw/artist/load_date=2026-02-02/artist.parquet
Loaded 59 rows → s3://media-retail-31415/raw/customer/load_date=2026-02-02/customer.parquet
Loaded 8 rows → s3://media-retail-31415/raw/employee/load_date=2026-02-02/employee.parquet
Loaded 25 rows → s3://media-retail-31415/raw/genre/load_date=2026-02-02/genre.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/invoice_line/load_date=2026-02-02/part_1000_invoice_line.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/invoice_line/load_date=2026-02-02/part_2000_invoice_line.parquet
Loaded 240 rows → s3://media-retail-31415/raw/invoice_line/load_date=2026-02-02/part_2240_invoice_line.parquet
Loaded 5 rows → s3://media-retail-31415/raw/media_type/load_date=2026-02-02/media_type.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_0_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_1000_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_2000_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_3000_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_4000_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_5000_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_6000_playlist_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_7000_playlist_track.parquet
Loaded 715 rows → s3://media-retail-31415/raw/playlist_track/load_date=2026-02-02/part_8000_playlist_track.parquet
Loaded 18 rows → s3://media-retail-31415/raw/playlist/load_date=2026-02-02/playlist.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/track/load_date=2026-02-02/part_1000_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/track/load_date=2026-02-02/part_2000_track.parquet
Loaded 1,000 rows → s3://media-retail-31415/raw/track/load_date=2026-02-02/part_3000_track.parquet
Loaded 503 rows → s3://media-retail-31415/raw/track/load_date=2026-02-02/part_3503_track.parquet
```