import io

import boto3
import pandas as pd
from botocore.exceptions import ClientError


def load(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload a DataFrame to S3 as a Parquet file."""


    if df is None:
        print("DF is None. " + key)
        return

    if df.empty:
        print("Nothing to upload — DataFrame is empty. " + key)
        return
   
 
    s3 = boto3.client("s3")
    

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"Loaded {len(df):,} rows → s3://{bucket}/{key}")
        
    except ClientError as e:
        print(f"Upload failed: {e}")
        raise
