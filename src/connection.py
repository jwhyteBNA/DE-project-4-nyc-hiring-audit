import os
import logging 
import pandas as pd
import polars as pl
from io import BytesIO
from minio import Minio
import snowflake.connector
import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio.error import S3Error
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from snowflake.connector.pandas_tools import write_pandas


load_dotenv()

VS_CODE_WORKSPACE_FOLDER = os.getenv('VS_CODE_WORKSPACE_FOLDER')
LOG_DIR = os.path.join(VS_CODE_WORKSPACE_FOLDER, 'src/logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, 'connection.log')

logging.getLogger("connection").handlers.clear()

logger = logging.getLogger("connection")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("Logger initialized for connection module.")

def strip_timestamp_and_extension(filename: str) -> str:
    base = os.path.basename(filename)
    parts = base.split('_')
    if len(parts) > 6:
        return '_'.join(parts[:-6])
    else:
        return base.replace('.parquet', '')

def process_minio_files(obj, minio_client, conn, snowflake_db, snowflake_schema, batch_threshold=150000):
    logger.info(f"Processing file: {obj.object_name} from MinIO.")
    try:
        minio_response = minio_client.get_object(obj.bucket_name, obj.object_name)
        parquet_bytes = BytesIO(minio_response.read())
        minio_response.close()
        minio_response.release_conn()

        parquet_file = pq.ParquetFile(parquet_bytes)
        total_rows = parquet_file.metadata.num_rows

        table_name = strip_timestamp_and_extension(obj.object_name)

        if total_rows > batch_threshold:
            logger.info(f"File {obj.object_name} is large ({total_rows} rows), processing first 150,000 rows in batches.")
            try:
                first_batch = next(parquet_file.iter_batches(batch_size=batch_threshold))
                batch_large_data = pl.from_arrow(first_batch).with_columns([
                    pl.lit(obj.object_name).alias("source"),
                    pl.lit(datetime.now(timezone.utc)).alias("original_load_time")
                ])
                logger.info(f"Writing batch to Snowflake table: {snowflake_db}.{snowflake_schema}.{table_name}")
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=batch_large_data.to_pandas(),
                    table_name=table_name,
                    database=snowflake_db,
                    schema=snowflake_schema,
                    auto_create_table=True,
                    overwrite=True,
                    quote_identifiers=True,
                    use_logical_type=True
                )
                if success:
                    logger.info(f"Successfully wrote {nrows} rows in {nchunks} chunks to Snowflake table: {snowflake_db}.{snowflake_schema}.{table_name}")
                else:
                    logger.error(f"Failed to write to Snowflake table: {snowflake_db}.{snowflake_schema}.{table_name}")
            except StopIteration:
                logger.info(f"No more batches to process for file: {obj.object_name}")
        else:
            logger.info(f"File {obj.object_name} is small ({total_rows} rows), processing all rows.")
            data_from_minio = pl.read_parquet(parquet_bytes)
            data_from_minio = data_from_minio.with_columns([
                pl.lit(obj.object_name).alias("source"),
                pl.lit(datetime.now(timezone.utc)).alias("original_load_time")
            ])
            logger.info(f"Writing all rows to Snowflake table: {snowflake_db}.{snowflake_schema}.{table_name}")

            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=data_from_minio.to_pandas(),
                table_name=table_name,
                database=snowflake_db,
                schema=snowflake_schema,
                auto_create_table=True,
                overwrite=True,
                quote_identifiers=True,
                use_logical_type=True
            )

            if success:
                logger.info(f"Successfully wrote {nrows} rows in {nchunks} chunks to Snowflake table: {snowflake_db}.{snowflake_schema}.{table_name}")
            else:
                logger.error(f"Failed to write to Snowflake table: {snowflake_db}.{snowflake_schema}.{table_name}")

    except S3Error as e:
        logger.error(f"Failed to process file: {obj.object_name} from MinIO: {e}")
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty data error for file: {obj.object_name} from MinIO: {e}")
    except Exception as e:
        logger.error(f"An error occurred while processing file: {obj.object_name} from MinIO: {e}")


def main():
    MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL')
    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_BASE_SCHEMA')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

    minio_client = None
    conn = None

    try:
        minio_client = Minio(
            MINIO_EXTERNAL_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info("Connected to MinIO successfully.")
    except S3Error as e:
        logger.error(f"Failed to connect to MinIO: {e}")


    conn = snowflake.connector.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = SNOWFLAKE_SCHEMA,
        role = SNOWFLAKE_ROLE
    )
    sf_cursor = conn.cursor()
    logger.info("Connected to Snowflake successfully!")

    objects_to_process = minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True)

    for obj in objects_to_process:
        process_minio_files(obj, minio_client, conn, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)

    if 'sf_cursor' in locals() and sf_cursor:
        sf_cursor.close()
        logger.info("Snowflake cursor closed.")
    if conn:
        conn.close()
    logger.info("Snowflake connection closed.")

if __name__ == "__main__":
    main()