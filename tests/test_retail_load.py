# tests/test_retail_load.py
import pandas as pd

import pytest

from src.etl.retail import load as load_module


def test_load_retail_to_db_calls_loader_in_correct_order(monkeypatch, caplog):
    products_df = pd.DataFrame({"product_id": [1, 2]})
    stores_df = pd.DataFrame({"store_id": [10]})
    sales_df = pd.DataFrame({"sale_id": [100, 101, 102]})
    enriched_df = pd.DataFrame({"col": [1]})

    calls = []

    def fake_load_dataframe_to_table(df, table_name, mode="overwrite"):
        calls.append(
            {
                "df_len": len(df),
                "table_name": table_name,
                "mode": mode,
            }
        )

    monkeypatch.setattr(
        load_module,
        "load_dataframe_to_table",
        fake_load_dataframe_to_table,
        raising=True,
    )

    batch_size = 500
    with caplog.at_level("INFO"):
        load_module.load_retail_to_db(
            sales_clean=sales_df,
            products_clean=products_df,
            stores_clean=stores_df,
            enriched=enriched_df,
            batch_size=batch_size,
        )

    # 4 tables loaded
    assert len(calls) == 4

    # Order as implemented: products, stores, sales, enriched
    assert calls[0]["table_name"] == "retail_products_clean"
    assert calls[1]["table_name"] == "retail_stores_clean"
    assert calls[2]["table_name"] == "retail_sales_clean"
    assert calls[3]["table_name"] == "retail_sales_enriched"

    # All use overwrite mode
    assert all(c["mode"] == "overwrite" for c in calls)

    # Lengths flow through correctly
    assert calls[0]["df_len"] == len(products_df)
    assert calls[1]["df_len"] == len(stores_df)
    assert calls[2]["df_len"] == len(sales_df)
    assert calls[3]["df_len"] == len(enriched_df)

    # batch_size appears in log as documented
    text = caplog.text
    assert "batch_size=500" in text
    assert "NOTE: batch_size=500 is currently only logged" in text
