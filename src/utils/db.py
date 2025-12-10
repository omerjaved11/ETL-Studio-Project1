from typing import List, Dict, Optional, Iterable
import psycopg
from psycopg.rows import dict_row
from typing import Iterable
from src.utils.config import config
from src.utils.logger import get_logger
import pandas as pd
from psycopg import sql

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



def update_data_source_shape(source_id: int, row_count: int, column_count: int) -> None:
    """
    Update row_count and column_count for a given data source.
    """
    sql = """
    UPDATE data_sources
    SET row_count = %s,
        column_count = %s
    WHERE id = %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (row_count, column_count, source_id))
            conn.commit()
        logger.info(
            "Updated data_source id=%s with shape=(%d, %d)",
            source_id, row_count, column_count
        )
    except Exception:
        logger.exception("Failed to update data source shape for id=%s", source_id)
        raise


def delete_data_sources(ids: Iterable[int]) -> None:
    """
    Delete one or more data_sources records by ID.
    (Does NOT touch files; routes will handle file/df cleanup.)
    """
    ids = list(ids)
    if not ids:
        return

    sql = "DELETE FROM data_sources WHERE id = %s"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for sid in ids:
                    cur.execute(sql, (sid,))
            conn.commit()
        logger.info("Deleted %d data_sources rows: %s", len(ids), ids)
    except Exception:
        logger.exception("Failed to delete data sources: %s", ids)
        raise

def get_data_source_by_id(source_id: int) -> Optional[Dict]:
    """
    Fetch a single data_source row by ID as a dict, or None if not found.
    """
    sql = """
    SELECT id, name, source_type, original_name, file_path,
           row_count, column_count, status, created_at
    FROM data_sources
    WHERE id = %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (source_id,))
                row = cur.fetchone()
                if not row:
                    return None
                col_names = [desc[0] for desc in cur.description]
        return dict(zip(col_names, row))
    except Exception:
        logger.exception("Failed to fetch data source by id=%s", source_id)
        raise

def load_dataframe_to_table(
    df: pd.DataFrame,
    table_name: str,
    mode: str = "overwrite",
):
    """
    Load a pandas DataFrame into a PostgreSQL table.

    mode:
        - "overwrite": drop table if exists, create new, insert all rows
        - "append": create table if not exists, append rows
    """
    if df.empty:
        logger.warning("[DB-LOAD] DataFrame for table '%s' is empty. Nothing to load.", table_name)
        return

    # Validate table name (very basic)
    if not str(table_name).replace("_", "").isalnum():
        raise ValueError(f"Invalid table name: {table_name}")

    logger.info("[DB-LOAD] Loading DataFrame into table '%s' (mode=%s)", table_name, mode)

    # Infer PostgreSQL column types
    def infer_pg_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMPTZ"
        return "TEXT"

    columns = list(df.columns)
    col_types = [infer_pg_type(df[col].dtype) for col in columns]

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            # DROP table if overwrite
            if mode == "overwrite":
                drop_stmt = sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(table_name)
                )
                cur.execute(drop_stmt)
                logger.info("[DB-LOAD] Dropped existing table '%s'", table_name)

            # CREATE TABLE (if append, this is IF NOT EXISTS)
            if mode == "append":
                create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(
                        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type))
                        for col, col_type in zip(columns, col_types)
                    ),
                )
            else:
                create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(
                        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type))
                        for col, col_type in zip(columns, col_types)
                    ),
                )

            cur.execute(create_stmt)
            logger.info("[DB-LOAD] Created table '%s' with schema.", table_name)

            # Prepare INSERT statement
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
            insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                placeholders,
            )

            # Insert all rows
            rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
            cur.executemany(insert_stmt, rows)

            logger.info("[DB-LOAD] Inserted %d rows into '%s'", len(rows), table_name)

        conn.commit()
        logger.info("[DB-LOAD] Commit successful for table '%s'", table_name)
