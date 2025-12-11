# tests/test_retail_run.py
import pandas as pd

from src.etl.retail.run import run_retail_etl, RetailETLResult
from src.etl.retail import run as run_module


def _df_with_shape(rows: int, cols: int) -> pd.DataFrame:
    # Simple helper to create a df with a specific shape
    data = {f"c{i}": [i] * rows for i in range(cols)}
    return pd.DataFrame(data)


def test_run_retail_etl_orchestrates_and_returns_shapes(monkeypatch, caplog):
    # --- fake extract output (raw) ---
    sales_raw = _df_with_shape(2, 3)      # (2, 3)
    products_raw = _df_with_shape(3, 2)   # (3, 2)
    stores_raw = _df_with_shape(4, 1)     # (4, 1)

    def fake_extract_retail():
        return sales_raw, products_raw, stores_raw

    # --- fake transform output (clean) ---
    sales_clean = _df_with_shape(5, 4)      # (5, 4)
    products_clean = _df_with_shape(6, 3)   # (6, 3)
    stores_clean = _df_with_shape(7, 2)     # (7, 2)

    def fake_transform_sales(df):
        assert df is sales_raw
        return sales_clean

    def fake_transform_products(df):
        assert df is products_raw
        return products_clean

    def fake_transform_stores(df):
        assert df is stores_raw
        return stores_clean

    # --- fake join output (enriched) ---
    enriched = _df_with_shape(10, 8)  # (10, 8)

    def fake_join_sales_products_stores(s_df, p_df, st_df):
        assert s_df is sales_clean
        assert p_df is products_clean
        assert st_df is stores_clean
        return enriched

    # --- fake load to DB ---
    load_calls = {}

    def fake_load_retail_to_db(
        *,
        sales_clean,
        products_clean,
        stores_clean,
        enriched,
        batch_size,
    ):
        load_calls["sales_clean"] = sales_clean
        load_calls["products_clean"] = products_clean
        load_calls["stores_clean"] = stores_clean
        load_calls["enriched"] = enriched
        load_calls["batch_size"] = batch_size

    # Patch everything on the run module (where run_retail_etl uses them)
    monkeypatch.setattr(run_module, "extract_retail", fake_extract_retail, raising=True)
    monkeypatch.setattr(run_module, "transform_sales", fake_transform_sales, raising=True)
    monkeypatch.setattr(run_module, "transform_products", fake_transform_products, raising=True)
    monkeypatch.setattr(run_module, "transform_stores", fake_transform_stores, raising=True)
    monkeypatch.setattr(
        run_module,
        "join_sales_products_stores",
        fake_join_sales_products_stores,
        raising=True,
    )
    monkeypatch.setattr(
        run_module,
        "load_retail_to_db",
        fake_load_retail_to_db,
        raising=True,
    )

    batch_size = 123
    with caplog.at_level("INFO"):
        result = run_retail_etl(batch_size=batch_size)

    # Returned dataclass type + shapes
    assert isinstance(result, RetailETLResult)
    assert result.sales_raw_shape == sales_raw.shape
    assert result.products_raw_shape == products_raw.shape
    assert result.stores_raw_shape == stores_raw.shape
    assert result.sales_clean_shape == sales_clean.shape
    assert result.products_clean_shape == products_clean.shape
    assert result.stores_clean_shape == stores_clean.shape
    assert result.enriched_shape == enriched.shape

    # Load was called with the transformed / enriched data and correct batch size
    assert load_calls["sales_clean"] is sales_clean
    assert load_calls["products_clean"] is products_clean
    assert load_calls["stores_clean"] is stores_clean
    assert load_calls["enriched"] is enriched
    assert load_calls["batch_size"] == batch_size

    # Optional: make sure the "pipeline completed successfully" log happened
    assert "pipeline completed successfully" in caplog.text


def test_run_retail_etl_default_batch_size(monkeypatch):
    # Minimal smoke test to ensure default batch_size=1000 is passed through
    sales_raw = _df_with_shape(1, 1)
    products_raw = _df_with_shape(1, 1)
    stores_raw = _df_with_shape(1, 1)

    monkeypatch.setattr(
        run_module,
        "extract_retail",
        lambda: (sales_raw, products_raw, stores_raw),
        raising=True,
    )
    monkeypatch.setattr(
        run_module,
        "transform_sales",
        lambda df: df,
        raising=True,
    )
    monkeypatch.setattr(
        run_module,
        "transform_products",
        lambda df: df,
        raising=True,
    )
    monkeypatch.setattr(
        run_module,
        "transform_stores",
        lambda df: df,
        raising=True,
    )
    monkeypatch.setattr(
        run_module,
        "join_sales_products_stores",
        lambda s, p, st: s,
        raising=True,
    )

    captured = {}

    def fake_load_retail_to_db(
        *,
        sales_clean,
        products_clean,
        stores_clean,
        enriched,
        batch_size,
    ):
        captured["batch_size"] = batch_size

    monkeypatch.setattr(
        run_module,
        "load_retail_to_db",
        fake_load_retail_to_db,
        raising=True,
    )

    run_retail_etl()  # use default
    assert captured["batch_size"] == 1000
