from typing import Dict, List, Optional
import psycopg
from psycopg.rows import dict_row

from src.utils.config import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

def get_db_connection():
    db = config["database"]
    try:
        conn = psycopg.connect(
            host=db["host"],
            port=db["port"],
            dbname=db["name"],
            user=db["user"],
            password=db["password"],
        )
        logger.info("Successfully connected to database")
        return conn
    except Exception as e:
        logger.exception("Exception occurred while connecting to database")
        return None

def init_metadata_tables() -> None:
    """
    Ensure the data_sources table exists.
    Call this once at app startup.
    """

    create_sql = """
    Create table if not exists data_sources(
        id              serial primary key,
        name            text not null,
        source_type     text not null,
        original_name   text,
        file_path       text,
        row_count       integer,
        column_count    integer,
        status          text not null default 'ready',
        created_at      timestamptz not null default now()

    );
"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
            conn.commit()
        logger.info("Ensured data_sources table exists")
    except:
        logger.exception("Failed to create/verify data_sources table")

def insert_data_source(
        name: str,
        source_type: str,
        orignial_name: Optional[str],
        file_path: Optional[str],
        row_count: Optional[int],
        column_count: Optional[int],
        status: str = "ready") -> int:
    """
    Insert a new row into data_source table;
    """
    sql = """
        Insert into data_sources (name,source_type, original_name, file_path, row_count, column_count, status)
        Values (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (name, source_type, orignial_name,
                     file_path, row_count, column_count, status),)
                row = cur.fetchone()
                if not row:
                    raise logger.exception("No ID returned from from insert into data_sources")
                new_id = row[0]
            conn.commit()
        logger.info("Inserted data_source id=%s name=%s, type = %s", new_id, name, source_type)
        return new_id
    except:
        logger.exception("Failed to insert data source")
        raise

def get_all_data_sources()-> List[Dict]:
    """
    Return list of all data sources ordered by created_at desc.
    """
    sql = """
    SELECT id, name, source_type, original_name, row_count, column_count, status, created_at
    FROM data_sources
    ORDER BY created_at DESC;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]
        result = [dict(zip(col_names, row)) for row in rows]
        return result
    except:
        logger.exception("Failed to fetch data sources table")
        raise

def update_source_filepath(source_id: int, target_file: str):
    """
    Updated file path.
    """
    sql = """
    update data_sources set file_path = %s where id = %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(str(target_file),source_id))
    except psycopg.Error as e:
        logger.exception(f"Error executing update query: {e}")
        # logger.exception("Failed to updated data sources table")
        raise