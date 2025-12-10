# tests/test_db_load_dataframe.py
import pandas as pd
import pytest

from src.utils import db as db_module


class DummyCursor:
    def __init__(self):
        self.executed = []
        self.executemany_calls = []

    def execute(self, stmt, params=None):
        # Just store statement (as string) and params for assertions
        self.executed.append((str(stmt), params))

    def executemany(self, stmt, seq_of_params):
        self.executemany_calls.append((str(stmt), list(seq_of_params)))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConnection:
    def __init__(self):
        self.cursors = []
        self.commits = 0

    def cursor(self):
        c = DummyCursor()
        self.cursors.append(c)
        return c

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def dummy_conn(monkeypatch):
    conn = DummyConnection()

    def fake_get_db_connection():
        return conn

    monkeypatch.setattr(db_module, "get_db_connection", fake_get_db_connection)
    return conn


def test_load_dataframe_to_table_overwrite_mode(sample_df, dummy_conn):
    """
    - In overwrite mode, table is dropped and recreated.
    - Rows are inserted with executemany.
    """
    table_name = "test_table"

    db_module.load_dataframe_to_table(sample_df, table_name=table_name, mode="overwrite")

    # We should have one connection, one cursor used
    assert len(dummy_conn.cursors) == 1
    cursor = dummy_conn.cursors[0]

    # At least 2 execute calls: DROP TABLE, CREATE TABLE, plus INSERT via executemany
    executed_stmts = [stmt for stmt, _ in cursor.executed]
    assert any("DROP TABLE IF EXISTS" in s for s in executed_stmts)
    assert any("CREATE TABLE" in s for s in executed_stmts)

    assert len(cursor.executemany_calls) == 1
    insert_stmt, rows = cursor.executemany_calls[0]
    assert "INSERT INTO" in insert_stmt
    # 3 rows in sample_df
    assert len(rows) == 3

    # commit should be called once
    assert dummy_conn.commits == 1


def test_load_dataframe_to_table_append_mode_creates_if_not_exists(sample_df, dummy_conn):
    """
    In append mode:
      - Should not DROP TABLE
      - Should CREATE TABLE IF NOT EXISTS
    """
    table_name = "test_append_table"

    db_module.load_dataframe_to_table(sample_df, table_name=table_name, mode="append")

    cursor = dummy_conn.cursors[-1]  # last used cursor
    executed_stmts = [stmt for stmt, _ in cursor.executed]

    # No plain DROP TABLE in append mode
    assert not any("DROP TABLE IF EXISTS" in s for s in executed_stmts)

    # CREATE TABLE IF NOT EXISTS must appear
    assert any("CREATE TABLE IF NOT EXISTS" in s for s in executed_stmts)

    # rows still inserted
    assert len(cursor.executemany_calls) == 1
    _, rows = cursor.executemany_calls[0]
    assert len(rows) == 3


def test_load_dataframe_to_table_empty_dataframe_skips(dummy_conn):
    """
    If DataFrame is empty, the function should log a warning and return
    without executing any SQL.
    """
    df_empty = pd.DataFrame(columns=["a", "b"])
    table_name = "empty_table"

    db_module.load_dataframe_to_table(df_empty, table_name=table_name, mode="overwrite")

    # No cursor should have been created because we returned early
    assert len(dummy_conn.cursors) == 0
    assert dummy_conn.commits == 0


def test_load_dataframe_to_table_invalid_table_name_raises(sample_df, dummy_conn):
    """
    Table names with non-alnum/underscore should trigger ValueError.
    """
    with pytest.raises(ValueError):
        db_module.load_dataframe_to_table(sample_df, table_name="bad-name!", mode="overwrite")

    # No SQL run, no commits
    assert len(dummy_conn.cursors) == 0
    assert dummy_conn.commits == 0
