import io
import pandas as pd
from google.cloud import storage
from prefect import task
from prefect_gcp import GcsBucket
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
async def upload_csv_to_gcs(bucket_name: str, df: pd.DataFrame, gcs_key: str):
    
    # old way before using prefect
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_key)
    blob.upload_from_string(buffer.getvalue(), content_type="text/csv")
    #blob.upload_from_filename(source_file_path)
    
    
    # gcs_bucket_block = await GcsBucket.load(bucket_name)
    # await gcs_bucket_block.upload_from_path(from_path=source_file_path, to_path=gcs_key)
    # print(f"File {source_file_path} uploaded")


