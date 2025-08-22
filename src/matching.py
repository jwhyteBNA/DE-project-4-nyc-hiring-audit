import os
import logging
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from rapidfuzz import process, fuzz
from logging.handlers import RotatingFileHandler
from snowflake.connector.pandas_tools import write_pandas
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

load_dotenv()

GOLD_SCHEMA = os.getenv("SNOWFLAKE_LOGIC_SCHEMA")

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "schema": os.getenv("SNOWFLAKE_CLEAN_SCHEMA")
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(os.path.join('src/logs', 'matching.log'), maxBytes=10*1024*1024, backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def load_silver_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, FISCAL_YEAR, TITLE_DESCRIPTION, ANNUALIZED_SALARY FROM CLEANED_PAYROLL")
        payroll = cursor.fetch_pandas_all()

        cursor.execute(
            """
            SELECT ID, BUSINESS_TITLE, CIVIL_SERVICE_TITLE,
                PAYROLL_SALARY_MIN_ANNUAL, PAYROLL_SALARY_MAX_ANNUAL,
                POSTING_DATE_NORM, POST_UNTIL_NORM
            FROM CLEANED_WEEKLY_JOBS
            """)
        weekly = cursor.fetch_pandas_all()

        cursor.execute(
            """
            SELECT ID, JOB_TITLE, MEDIAN_POSTING_DURATION_DAYS FROM CLEANED_TOP_TITLES
            """
        )
        logger.info("Loaded silver tables from Snowflake")
        top = cursor.fetch_pandas_all()
        return payroll, weekly, top
    except Exception as e:
        logger.error(f"Failed to read tables from Snowflake: {e}")
        return None
    finally:
        cursor.close()


def calculate_fuzzy_match(source_col, target_col, use_rapidfuzz=True):
    matches=[]
    for source in source_col.unique():
        best_match = process.extractOne(
            query=source,
            choices=target_col.unique(),
            scorer=fuzz.WRatio if use_rapidfuzz else fuzz.ratio
        )
        if best_match:
            match_val, score, _ = best_match
            matches.append((source, match_val, score))
    return pd.DataFrame(matches, columns=["SOURCE_TITLE", "TARGET_TITLE", "RATIO"])


def ensure_gold_table(conn, table_name, schema, columns_types):
    cursor = conn.cursor()
    col_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_types.items()])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({col_defs})')
    cursor.close()

def fuzzy_enrich_postings(postings, payroll, min_ratio=85):
    results = []
    payroll_titles = payroll["TITLE_DESCRIPTION"].unique()
    for _, row in postings.iterrows():
        business_title = row["BUSINESS_TITLE"]
        match = process.extractOne(
            business_title,
            payroll_titles,
            scorer=fuzz.token_set_ratio
        )
        if match and match[1] >= min_ratio:
            matched_title = match[0]
            match_score = match[1]
            annualized_salary = payroll.loc[payroll["TITLE_DESCRIPTION"] == matched_title, "ANNUALIZED_SALARY"].values
            annualized_salary = annualized_salary[0] if len(annualized_salary) > 0 else None
        else:
            matched_title = None
            match_score = None
            annualized_salary = None

        results.append({
            "POSTING_ID": row["ID"],
            "BUSINESS_TITLE": business_title,
            "CIVIL_SERVICE_TITLE": row["CIVIL_SERVICE_TITLE"],
            "PAYROLL_MATCHED_TITLE": matched_title,
            "MATCH_SCORE": match_score,
            "POSTING_DURATION": row.get("POSTING_DURATION"),
            "PAYROLL_ANNUALIZED_SALARY": annualized_salary,
            "PAYROLL_SALARY_MIN_ANNUAL": row["PAYROLL_SALARY_MIN_ANNUAL"],
            "PAYROLL_SALARY_MAX_ANNUAL": row["PAYROLL_SALARY_MAX_ANNUAL"],
            "POSTING_DATE_NORM": row["POSTING_DATE_NORM"],
            "POST_UNTIL_NORM": row["POST_UNTIL_NORM"],
        })
    return pd.DataFrame(results)

def write_to_snowflake(conn, df, table_name, schema):
    dtype_map = {
        "int64": "NUMBER",
        "float64": "FLOAT",
        "object": "STRING",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN"
    }
    col_types = {col: dtype_map.get(str(dtype), "STRING") for col, dtype in df.dtypes.items()}
    ensure_gold_table(conn, table_name, schema, col_types)
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name, schema=schema)
    logger.info(f"Wrote {nrows} rows to {schema}.{table_name} in Snowflake.")


def main():
    conn = get_connection()
    payroll, weekly, top = load_silver_tables(conn)

    weekly["POSTING_DATE_NORM"] = pd.to_datetime(weekly["POSTING_DATE_NORM"], errors="coerce")
    weekly["POST_UNTIL_NORM"] = pd.to_datetime(weekly["POST_UNTIL_NORM"], errors="coerce")

    weekly["POSTING_DURATION"] = (weekly["POST_UNTIL_NORM"] - weekly["POSTING_DATE_NORM"]).dt.days

    weekly = weekly[
        (weekly["POSTING_DATE_NORM"].dt.year >= 2024) & 
        (weekly["POSTING_DATE_NORM"].dt.year <= 2025) &
        (weekly["POSTING_DURATION"] >= 0) &
        (weekly["POSTING_DURATION"] <= 365)
]

    payroll = payroll[payroll["FISCAL_YEAR"].isin([2024, 2025])]

    
    enriched_postings = fuzzy_enrich_postings(weekly, payroll, min_ratio=85)
    enriched_postings = enriched_postings[
        (enriched_postings["MATCH_SCORE"].notna()) &
        (enriched_postings["MATCH_SCORE"] >= 85)
    ]
    write_to_snowflake(conn, enriched_postings, "NYC_JOB_POSTINGS_PAYROLL_MATCHES", GOLD_SCHEMA)


if __name__ == "__main__":
    main()
