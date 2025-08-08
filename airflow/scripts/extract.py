import logging
import os
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import List, Dict


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)



# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))


# Functions
def extract_page_data(url: str, params: dict) -> List[Dict]:
    """
    Extract data from a single page of the API.

    Args:
        url (str): The full endpoint URL.
        params (dict): Query parameters, including the page number.

    Returns:
        list[dict]: A list of records from the page, or empty list if failed.
    """
    response = requests.get(url, params=params)

    if response.status_code != 200:
        logger.warning(f'Failed to fetch page {params.get("page")}, status code: {response.status_code}')
        return []

    return response.json().get('results', [])


def extract_all_data(base_url: str, endpoint: str) -> pd.DataFrame:
    """
    Extracts all paginated data from a given API endpoint.

    Args:
        base_url (str): The root URL of the API.
        endpoint (str): The specific endpoint to extract data from.

    Returns:
        pd.DataFrame: A DataFrame containing all extracted data.
    """
    url = f'{base_url}/{endpoint}'
    page = 1
    collected_data = []

    while True:
        params = {'page': page}
        page_data = extract_page_data(url, params=params)

        if not page_data:
            logger.info(f'No more data found for endpoint "{endpoint}" at page {page}.')
            break

        collected_data.extend(page_data)
        page += 1
    
    logger.info(f'{len(collected_data)} records collected from endpoint "{endpoint}".')
    return pd.DataFrame(collected_data)


def save_df_on_minio_bucket(df: pd.DataFrame, bucket: str, bkt_filepath: str, minio_config: dict) -> None:
    """
    Uploads a DataFrame as a CSV file to a MinIO bucket.

    Args:
        df (pd.DataFrame): Data to be saved.
        bucket (str): The name of the bucket.
        bkt_filepath (str): The destination path inside the bucket.
        minio_config (dict): MinIO connection config (endpoint, access_key, secret_key).

    Returns:
        None
    """
    s3 = boto3.client(
        's3',
        endpoint_url=minio_config['endpoint_url'],
        aws_access_key_id=minio_config['access_key'],
        aws_secret_access_key=minio_config['secret_key']
    )

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=bkt_filepath, Body=csv_buffer.getvalue())
    logger.info(f'FIle uploaded to bucket "{bucket}" at path: {bkt_filepath}')


def generate_upload_bkt_path(endpoint: str) -> str:
    """
    Generates a structured file path for the uploaded CSV based on the current date.

    Args:
        endpoint (str): The API endpoint name used as folder name.
    
    Returns:
        str: the complete path inside the bucket.
    """
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    timestamp = now.strftime("%Y-%m-%d_%H%M%S")

    filename = f'{endpoint}_{timestamp}.csv'
    path = f'{endpoint}/{year}/{month}/{filename}'

    return path


def extract_data_from_all_endpoints() -> None:
    """
    Extracts data from all defined endpoints and uploads the resulting DataFrames to a MinIO bucket.

    Endpoints are paginated and fetched one by one. Each CSV is saved to a structured folder based on year/month.
    """
    ENDPOINTS = [
        'addresses',
        'business_partners',
        'employees',
        'product_categories',
        'product_category_text',
        'product_texts',
        'products',
        'sales_order_items',
        'sales_orders'
    ]

    # Load MinIO configuration from environment variables
    minio_config = {
        'endpoint_url': 'http://minio:9000',
        'access_key': os.getenv('MINIO_ROOT_USER'),
        'secret_key': os.getenv('MINIO_ROOT_PASSWORD')
    }


    # Basic credentials check
    if not minio_config['access_key'] or not minio_config['secret_key']:
        logger.error('MinIO credentials are missing. Check your environment variables')
        return
    
    url = 'http://sap-api:8000'


    for endpoint in ENDPOINTS:
        try:
            logger.info(f'Extracting data from endpoint: {endpoint}')
            df_endpoint = extract_all_data(url, endpoint)

            if df_endpoint.empty:
                logger.warning(f'No data extracted for endpoint: {endpoint}')
                continue

            bkt_filepath = generate_upload_bkt_path(endpoint)
            save_df_on_minio_bucket(df_endpoint, 'staging', bkt_filepath, minio_config)

        except Exception as e:
            logger.exception(f'Error processing endpoint "{endpoint}": {str(e)}')
