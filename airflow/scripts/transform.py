from minio import Minio
from datetime import datetime
import pandas as pd
from io import BytesIO
import logging

from transformers import (
    addresses,
    business_partners,
    employees,
    product_categories,
    product_category_text,
    product_texts,
    products,
    sales_order_items,
    sales_orders
)

from load import upload_dataframe_to_postgres

# =============================
# Logging configuration
# =============================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================
# MinIO Connection
# =============================
def connect_to_bucket(url: str, access_key: str, secret_key: str) -> Minio:
    """
    Connects to a MinIO bucket.

    Args:
        url (str): MinIO endpoint URL.
        access_key (str): MinIO access key.
        secret_key (str): MinIO secret key.

    Returns:
        Minio: A MinIO client instance.
    """
    return Minio(
        endpoint=url,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )


# =============================
# Get Latest Object
# =============================
def get_latest_object_name_for_endpoint(client: Minio, bucket_name: str, endpoint: str) -> str:
    """
    Retrieves the most recent object name for a given endpoint in the MinIO bucket.

    Args:
        client (Minio): MinIO client instance.
        bucket_name (str): Name of the bucket.
        endpoint (str): Data endpoint prefix (e.g., 'sales_orders').

    Returns:
        str: Object name of the most recent file.
    """
    today = datetime.now()
    prefix = f'{endpoint}/{today.year}/{today.month:02}/{endpoint}_'

    objects = list(client.list_objects(bucket_name, prefix=prefix, recursive=True))

    if not objects:
        raise FileNotFoundError(f"No objects found for prefix '{prefix}' in bucket '{bucket_name}'")

    latest_object = max(objects, key=lambda obj: obj.object_name).object_name
    return latest_object


# =============================
# Download Object Data
# =============================
def get_data_from_latest_object(client: Minio, bucket_name: str, object_name: str) -> BytesIO:
    """
    Downloads a MinIO object and returns its content as a BytesIO buffer.

    Args:
        client (Minio): MinIO client instance.
        bucket_name (str): Name of the bucket.
        object_name (str): Object name to download.

    Returns:
        BytesIO: Binary stream of the object content.
    """
    response = client.get_object(bucket_name, object_name)
    data = BytesIO(response.read())
    response.close()
    response.release_conn()
    return data


# =============================
# Transform and Load Pipeline
# =============================
def transform_and_load_data_from_all_endpoints():
    """
    Executes the full transformation and loading pipeline:
      - Connects to MinIO
      - Retrieves the latest object for each endpoint
      - Transforms the object using the appropriate function
      - Uploads the transformed DataFrame to PostgreSQL
    """
    minio_host = 'minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minioadmin'
    bucket_name = 'staging'

    ENDPOINTS = {
        'addresses': addresses.transform,
        'business_partners': business_partners.transform,
        'employees': employees.transform,
        'product_categories': product_categories.transform,
        'product_category_text': product_category_text.transform,
        'product_texts': product_texts.transform,
        'products': products.transform, 
        'sales_order_items': sales_order_items.transform,
        'sales_orders': sales_orders.transform,
    }

    client = connect_to_bucket(minio_host, access_key, secret_key)

    for endpoint, transform_function in ENDPOINTS.items():
        try:
            logger.info(f'Processing endpoint: {endpoint}')
            object_name = get_latest_object_name_for_endpoint(client, bucket_name, endpoint)
            logger.info(f'Latest object found: {object_name}')

            raw_data = get_data_from_latest_object(client, bucket_name, object_name)
            df = transform_function(raw_data)

            logger.info(f'{endpoint} transformation complete.')
            logger.info(f'DataFrame shape: {df.shape}')

            upload_dataframe_to_postgres(df, endpoint, schema='bronze')
            logger.info(f'{endpoint} uploaded to Postgres.')

        except Exception as e:
            logger.error(f'Error processing endpoint "{endpoint}": {e}')
