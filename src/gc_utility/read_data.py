import pandas as pd
from google.cloud import storage
from prefect import task
import io

@task
async def read_csv_from_gcs(bucket_name: str, source_blob_name: str) -> pd.DataFrame:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    return df


# async def read_csv_from_gcs(bucket_block_name, source_blob_name):
#     # Cargamos el bloque de Prefect (el mismo que usas para subir)
#     gcs_bucket_block = await GcsBucket.load(bucket_block_name)
    
#     # Descargamos el contenido como bytes usando el bloque
#     content = await gcs_bucket_block.read_path(source_blob_name)
#     df = pd.read_csv(io.BytesIO(content))
#     return df


