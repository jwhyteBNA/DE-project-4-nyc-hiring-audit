import os
import time
import json
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


def get_api_last_updated(meta_url):
    meta = requests.get(meta_url).json()
    last_updated = meta.get("rowsUpdatedAt")
    if last_updated:
        return datetime.fromtimestamp(last_updated)
    return None

def get_last_ingest_time(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return datetime.fromisoformat(f.read().strip())
    return None

def set_last_ingest_time(dt, filepath):
    with open(filepath, "w") as f:
        f.write(dt.isoformat())


def conditional_ingest(api_name, meta_url, ingest_func, ingest_args, stamp_file):
    api_last_updated = get_api_last_updated(meta_url)
    last_ingest = get_last_ingest_time(stamp_file)
    if last_ingest is None or (api_last_updated and api_last_updated > last_ingest):
        logger.info(f"{api_name} data is new. Running ingestion.")
        timed_ingestion(ingest_func, *ingest_args)
        set_last_ingest_time(api_last_updated, stamp_file)
    else:
        logger.info(f"No new {api_name} data to ingest.")

def get_weekly_jobs_data(url, limit=1000):
    offset = 0
    all_data = []
    while True:
        response = requests.get(WEEKLY_JOBS_URL, params={"$limit": limit, "$offset": offset})
        if response.status_code == 200:
            logger.info("Weekly jobs data fetched successfully.")
            weekly_data = pl.read_csv(BytesIO(response.content))
            all_data.append(weekly_data)
            if weekly_data.shape[0] < limit:
                break
            offset += limit
        else:
            logger.error(f"Failed to fetch weekly jobs data: {response.status_code}")
            break
    return pl.concat(all_data) if all_data else pl.DataFrame()


def get_annual_payroll_data_streaming(url):
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    limit = 50000
    offset = 0
    parquet_path = f"ANNUAL_PAYROLL_{timestamp}.parquet"
    writer = None
    rows_downloaded = 0

    while True:
        logger.info(f"Requesting rows {offset} to {offset + limit} from API...")
        params = {"$limit": limit, "$offset": offset}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            batch_data = pl.read_csv(BytesIO(response.content), schema_overrides=PAYROLL_SCHEMA)
            logger.info(f"Fetched {batch_data.shape[0]} rows in this batch.")
            batch_data = batch_data.with_columns([
                pl.col("payroll_number").cast(pl.Float64),  
                pl.col("regular_hours").cast(pl.Float64),
                pl.col("ot_hours").cast(pl.Float64),
                pl.col("base_salary").cast(pl.Float64),
                pl.col("regular_gross_paid").cast(pl.Float64),
                pl.col("total_ot_paid").cast(pl.Float64),
                pl.col("total_other_pay").cast(pl.Float64)
            ])
            if batch_data.shape[0] == 0:
                break
            table = batch_data.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)
            rows_downloaded += batch_data.shape[0]
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
    logger.info(f"Extracting tab 'Job Postings Job Titles' from {filepath}")
    try:
        xls_data = pd.read_excel(filepath, sheet_name="Job Postings Job Titles", header=2, usecols="A:D")
        xls_polars = pl.from_pandas(xls_data)

        logger.info("Successfully extracted tab 'Job Postings Job Titles'.")
        return xls_polars
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
 # Metadata endpoints for Socrata APIs
    WEEKLY_META_URL = os.getenv('WEEKLY_META_URL')
    PAYROLL_META_URL = os.getenv('PAYROLL_META_URL')

    conditional_ingest(
        "Weekly Jobs",
        WEEKLY_META_URL,
        get_weekly_jobs_data,
        [WEEKLY_JOBS_URL],
        "ingest_stamps/weekly_jobs_last_ingest.txt"
    )
    conditional_ingest(
        "Annual Payroll",
        PAYROLL_META_URL,
        get_annual_payroll_data_streaming,
        [ANNUAL_PAYROLL_URL],
        "ingest_stamps/annual_payroll_last_ingest.txt"
    )

    top_jobs_data = timed_ingestion(extract_and_upload_xls_tabs, XLS_FILEPATH)
    if top_jobs_data is not None:
        upload_parquet_to_minio(top_jobs_data, "TOP_POSTED_TITLES.parquet")

   

if __name__ == "__main__":
    main()
