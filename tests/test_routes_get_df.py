# tests/test_routes_get_df.py
from pathlib import Path
import pandas as pd

from src.web import routes as routes_module


class DummyState:
    def __init__(self):
        self.df_store = {}
        # get_templates() expects this attribute; it won't actually use it
        # inside get_df in the non-error paths.
        self.templates = None


class DummyApp:
    def __init__(self):
        self.state = DummyState()


class DummyRequest:
    def __init__(self):
        self.app = DummyApp()


def test_get_df_returns_in_memory_without_disk_access(tmp_path, monkeypatch):
    """
    If df is already in df_store, get_df should simply return it without
    reading from disk.
    """
    request = DummyRequest()
    source_id = 1

    # Put a df into df_store
    df_in_memory = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    request.app.state.df_store[source_id] = df_in_memory

    # Monkeypatch DATA_SOURCES_DIR to a temp dir to ensure if it tried to read it would be here.
    monkeypatch.setattr(routes_module, "DATA_SOURCES_DIR", tmp_path)

    df = routes_module.get_df(request, source_id)

    assert df is df_in_memory  # same object
    assert request.app.state.df_store[source_id] is df_in_memory


def test_get_df_loads_from_disk_when_not_in_memory(tmp_path, monkeypatch):
    """
    If df is not present in df_store, get_df should read source_{id}.csv
    from DATA_SOURCES_DIR and store it in df_store.
    """
    request = DummyRequest()
    source_id = 2

    # Create a CSV file on disk for this source
    df_disk = pd.DataFrame({"x": [10, 20], "y": [30, 40]})
    csv_path = tmp_path / f"source_{source_id}.csv"
    df_disk.to_csv(csv_path, index=False)

    # Point routes.DATA_SOURCES_DIR at our temp dir
    monkeypatch.setattr(routes_module, "DATA_SOURCES_DIR", tmp_path)

    df_loaded = routes_module.get_df(request, source_id)

    # We should get a DataFrame equal to df_disk
    assert df_loaded is not None
    assert list(df_loaded.columns) == ["x", "y"]
    assert df_loaded.iloc[0]["x"] == 10

    # It should now also be cached in df_store
    assert source_id in request.app.state.df_store
    cached_df = request.app.state.df_store[source_id]
    assert cached_df.equals(df_loaded)


def test_get_df_returns_none_when_file_missing(tmp_path, monkeypatch, caplog):
    """
    If no in-memory df and no CSV on disk, get_df should return None.
    """
    request = DummyRequest()
    source_id = 999

    monkeypatch.setattr(routes_module, "DATA_SOURCES_DIR", tmp_path)

    df = routes_module.get_df(request, source_id)
    assert df is None
