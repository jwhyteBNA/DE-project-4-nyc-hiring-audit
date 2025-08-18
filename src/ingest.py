import os
import time
import logging
import requests
import numpy as np
import pandas as pd
import polars as pl
from io import BytesIO
from minio import Minio
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()

MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
minio_client = Minio(
    MINIO_EXTERNAL_URL,
    access_key=os.getenv('MINIO_ACCESS_KEY'),
    secret_key=os.getenv('MINIO_SECRET_KEY'),
    secure=False
)

WEEKLY_JOBS_URL = os.getenv('NYC_WEEKLY_JOBS_ENDPOINT')
ANNUAL_PAYROLL_URL = os.getenv('NYC_ANNUAL_PAYROLL_ENDPOINT')
XLS_FILEPATH = os.getenv('XLS_PATH')

PAYROLL_SCHEMA = {
    "fiscal_year": pl.Int64,
    "payroll_number": pl.Float64,
    "agency_name": pl.Utf8,
    "last_name": pl.Utf8,
    "first_name": pl.Utf8,
    "mid_init": pl.Utf8,
    "agency_start_date": pl.Utf8,  
    "work_location_borough": pl.Utf8,
    "title_description": pl.Utf8,
    "leave_status_as_of_june_30": pl.Utf8,
    "base_salary": pl.Float64,
    "pay_basis": pl.Utf8,
    "regular_hours": pl.Float64,
    "regular_gross_paid": pl.Float64,
    "ot_hours": pl.Float64,
    "total_ot_paid": pl.Float64,
    "total_other_pay": pl.Float64
}


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(os.path.join('src/logs', 'data_ingestion.log'), maxBytes=10*1024*1024, backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def timed_ingestion(func, *args, **kwargs):
    start = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    logger.info(f"{func.__name__} completed in {elapsed:.2f} seconds.")
    return result


def get_weekly_jobs_data(url):
    response = requests.get(WEEKLY_JOBS_URL)
    if response.status_code == 200:
        logger.info("Weekly jobs data fetched successfully.")
        weekly_data = pl.read_csv(BytesIO(response.content))
        return weekly_data
    else:
        logger.error(f"Failed to fetch weekly jobs data: {response.status_code}")
        return pl.DataFrame()


def get_annual_payroll_data_streaming(url):
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    total_rows=150000
    limit = 50000
    offset = 0
    rows_downloaded = 0
    parquet_path = f"ANNUAL_PAYROLL_{timestamp}.parquet"
    writer = None

    while rows_downloaded < total_rows:
        params = {"$limit": limit, "$offset": offset}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            batch_df = pl.read_csv(BytesIO(response.content), schema_overrides=PAYROLL_SCHEMA)
            batch_df = batch_df.with_columns([
                pl.col("payroll_number").cast(pl.Float64),  
                pl.col("regular_hours").cast(pl.Float64),
                pl.col("ot_hours").cast(pl.Float64),
                pl.col("base_salary").cast(pl.Float64),
                pl.col("regular_gross_paid").cast(pl.Float64),
                pl.col("total_ot_paid").cast(pl.Float64),
                pl.col("total_other_pay").cast(pl.Float64)
            ])
            if batch_df.shape[0] == 0:
                break
            if rows_downloaded + batch_df.shape[0] > total_rows:
                batch_df = batch_df.head(total_rows - rows_downloaded)
            table = batch_df.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
            rows_downloaded += batch_df.shape[0]
            offset += limit
        else:
            logger.error(f"Failed to fetch batch: {response.status_code}")
            break

    if writer:
        writer.close()
        logger.info(f"Finished writing {rows_downloaded} rows to {parquet_path}")
        with open(parquet_path, "rb") as f:
            minio_client.put_object(
                MINIO_BUCKET_NAME,
                parquet_path,
                f,
                os.path.getsize(parquet_path),
                content_type='application/octet-stream'
            )
        logger.info(f"Successfully uploaded {parquet_path} to MinIO")
        os.remove(parquet_path)
        logger.info(f"Removed local file {parquet_path}")


def extract_and_upload_xls_tabs(filepath):
    logger.info(f"Extracting tab 16 from {filepath}")
    try:
        tab16_data = pd.read_excel(filepath, sheet_name=15, header=2, usecols="A:D")
        tab16_pl = pl.from_pandas(tab16_data)

        logger.info("Successfully extracted tab 16.")
        return tab16_pl
    except Exception as e:
        logger.error(f"Failed to read tabs from {filepath}: {e}")
        return None


def upload_parquet_to_minio(parquet: pl.DataFrame, filename: str):
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    timestamped_filename = f"{filename.rstrip('.parquet')}_{timestamp}.parquet"
    buffer = BytesIO()
    parquet.write_parquet(buffer)
    buffer.seek(0)
    logger.info(f"Uploading {timestamped_filename} to MinIO as Parquet")
    minio_client.put_object(
        MINIO_BUCKET_NAME,
        timestamped_filename,
        buffer,
        buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    logger.info(f"Successfully uploaded {timestamped_filename} to MinIO")


def main():
    weekly_data = timed_ingestion(get_weekly_jobs_data, WEEKLY_JOBS_URL)
    upload_parquet_to_minio(weekly_data, "WEEKLY_JOBS.parquet")

    tab16_pl = timed_ingestion(extract_and_upload_xls_tabs, XLS_FILEPATH)
    if tab16_pl is not None:
        upload_parquet_to_minio(tab16_pl, "TOP_POSTED_TITLES.parquet")

    timed_ingestion(get_annual_payroll_data_streaming, ANNUAL_PAYROLL_URL)
   

if __name__ == "__main__":
    main()
