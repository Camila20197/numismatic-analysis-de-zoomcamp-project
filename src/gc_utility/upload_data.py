from google.cloud import storage
from prefect import task
from prefect_gcp.cloud_storage import GcsBucket

@task()
async def upload_csv_to_gcs(bucket_name: str, source_file_path: str, gcs_key: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_key)
    blob.upload_from_filename(source_file_path)
    
    # gcs_bucket_block = await GcsBucket.load(bucket_name)
    # await gcs_bucket_block.upload_from_path(from_path=source_file_path, to_path=gcs_key)
    # print(f"File {source_file_path} uploaded")


