# tests/test_retail_transform.py

import pandas as pd
import pytest

from src.etl.retail.transform import (
    _standardize_key,
    _drop_all_na_columns,
    _remove_duplicates,
    _fill_na,
    _drop_na_rows,
    _remove_outliers_iqr,
    _parse_dates_if_present,
    transform_sales,
    transform_products,
    transform_stores,
    join_sales_products_stores,
)


# ---------- helpers ----------

def test_standardize_key_creates_target_column():
    df = pd.DataFrame(
        {
            "ProductID": [1, 2],
            "other": ["x", "y"],
        }
    )

    out = _standardize_key(df, ["product_id", "ProductID"], "product_id")

    # New standardized column exists
    assert "product_id" in out.columns
    assert list(out["product_id"]) == ["1", "2"]  # cast to str + strip

    # Original column is still present
    assert "ProductID" in out.columns
    # Non-key columns are unchanged
    assert list(out["other"]) == ["x", "y"]


def test_standardize_key_no_candidate_keeps_dataframe():
    df = pd.DataFrame({"col": [1, 2, 3]})
    out = _standardize_key(df, ["product_id"], "product_id")

    # No new column added
    assert "product_id" not in out.columns
    # DataFrame content stays the same
    pd.testing.assert_frame_equal(df, out)


def test_drop_all_na_columns_drops_only_all_na():
    df = pd.DataFrame(
        {
            "a": [1, 2],
            "b": [None, None],  # all NA
            "c": [3, None],      # partially NA, should remain
        }
    )

    out = _drop_all_na_columns(df, label="test")

    assert list(out.columns) == ["a", "c"]
    pd.testing.assert_series_equal(out["a"], df["a"])
    pd.testing.assert_series_equal(out["c"], df["c"])


def test_remove_duplicates_with_subset():
    df = pd.DataFrame(
        {
            "product_id": [1, 1, 2],
            "store_id": [10, 10, 20],
            "value": [100, 100, 200],
        }
    )

    out = _remove_duplicates(df, label="sales", subset=["product_id", "store_id"])

    # (1,10) appears twice, should be one row; (2,20) once
    assert len(out) == 2
    assert set(zip(out["product_id"], out["store_id"])) == {(1, 10), (2, 20)}


def test_fill_na_numeric_and_categorical():
    df = pd.DataFrame(
        {
            "num": [1.0, None, 3.0],
            "cat": ["a", None, "a"],
        }
    )

    out = _fill_na(df, label="test")

    # No NA remaining
    assert not out.isna().any().any()

    # Numeric NA filled with median of [1, 3] -> 2
    assert out.loc[1, "num"] == pytest.approx(2.0)

    # Categorical NA filled with mode -> "a"
    assert out.loc[1, "cat"] == "a"


def test_drop_na_rows_removes_rows_with_any_na():
    df = pd.DataFrame(
        {
            "a": [1, None, 2],
            "b": [3, 4, None],
        }
    )

    out = _drop_na_rows(df, label="test")

    # Only first row has no NA
    assert len(out) == 1
    assert out.index.tolist() == [0]
    assert out.iloc[0]["a"] == 1
    assert out.iloc[0]["b"] == 3


def test_remove_outliers_iqr_removes_extreme_values():
    df = pd.DataFrame({"x": [1, 1, 1, 1000]})

    out = _remove_outliers_iqr(df, label="test")

    # Extreme outlier 1000 should be removed
    assert len(out) == 3
    assert out["x"].max() == 1


def test_remove_outliers_iqr_no_numeric_columns_returns_unchanged():
    df = pd.DataFrame({"name": ["a", "b", "c"]})

    out = _remove_outliers_iqr(df, label="test")

    pd.testing.assert_frame_equal(df, out)


def test_parse_dates_if_present_converts_date_columns():
    df = pd.DataFrame(
        {
            "sale_date": ["2024-01-01", "2024-02-02"],
            "not_date": ["2024-03-03", "2024-04-04"],  # make this date-like too
        }
    )

    out = _parse_dates_if_present(df, label="sales")

    assert "sale_date" in out.columns
    assert "not_date" in out.columns
    assert str(out["sale_date"].dtype).startswith("datetime64")
    assert str(out["not_date"].dtype).startswith("datetime64")

# ---------- public transforms ----------

def test_transform_sales_basic_cleaning():
    sales_df = pd.DataFrame(
        {
            # Intentionally use alternate key names to test _standardize_key
            "ProductID": [101, 101, 102, None],
            "StoreID": [1, 1, 2, 2],
            "sale_date": ["2024-01-01", "2024-01-01", "2024-01-02", None],
            "amount": [10.0, 10.0, 20.0, 30.0],
            # all-NA column should be dropped
            "all_na": [None, None, None, None],
        }
    )

    out = transform_sales(sales_df)

    # Keys standardized
    assert "product_id" in out.columns
    assert "store_id" in out.columns

    # all-NA column removed
    assert "all_na" not in out.columns

    # Duplicates and NA rows removed: we should keep only two clean rows
    assert len(out) == 2

    # No NA should remain after dropping NA rows
    assert not out.isna().any().any()

    # Date column parsed
    assert "sale_date" in out.columns
    assert str(out["sale_date"].dtype).startswith("datetime64")


def test_transform_products_cleaning():
    products_df = pd.DataFrame(
        {
            "ProductID": [101, 101, None],
            "name": ["Widget", "Widget", None],
            "all_na": [None, None, None],
        }
    )

    out = transform_products(products_df)

    # Standardized product_id
    assert "product_id" in out.columns

    # all-NA column dropped
    assert "all_na" not in out.columns

    # One duplicate + one NA row -> only one remaining clean row
    assert len(out) == 1

    # No NA remaining
    assert not out.isna().any().any()


def test_transform_stores_cleaning():
    stores_df = pd.DataFrame(
        {
            "StoreID": [1, 1, None],
            "city": ["NYC", "NYC", None],
            "all_na": [None, None, None],
        }
    )

    out = transform_stores(stores_df)

    # Standardized store_id
    assert "store_id" in out.columns

    # all-NA column dropped
    assert "all_na" not in out.columns

    # One duplicate + one NA row -> only one remaining clean row
    assert len(out) == 1

    # No NA remaining
    assert not out.isna().any().any()


# ---------- join ----------

def test_join_sales_products_stores_happy_path():
    sales_df = pd.DataFrame(
        {
            "product_id": [101, 102],
            "store_id": [1, 2],
            "amount": [10.0, 20.0],
        }
    )
    products_df = pd.DataFrame(
        {
            "product_id": [101],
            "category": ["A"],
        }
    )
    stores_df = pd.DataFrame(
        {
            "store_id": [1],
            "city": ["NYC"],
        }
    )

    enriched = join_sales_products_stores(sales_df, products_df, stores_df)

    # Left-join semantics: keep all sales rows
    assert enriched.shape[0] == 2

    # Columns from products and stores present
    assert "category" in enriched.columns
    assert "city" in enriched.columns

    # First row matched both product and store
    row0 = enriched.iloc[0]
    assert row0["product_id"] == 101
    assert row0["store_id"] == 1
    assert row0["category"] == "A"
    assert row0["city"] == "NYC"

    # Second row has no matching product/store -> NaNs there
    row1 = enriched.iloc[1]
    assert row1["product_id"] == 102
    assert row1["store_id"] == 2
    assert pd.isna(row1["category"])
    assert pd.isna(row1["city"])


def test_join_sales_products_stores_missing_keys_no_crash():
    # If key columns missing on product/store frames, function should not blow up
    sales_df = pd.DataFrame({"amount": [10.0]})
    products_df = pd.DataFrame({"something_else": [1]})
    stores_df = pd.DataFrame({"another_col": [2]})

    enriched = join_sales_products_stores(sales_df, products_df, stores_df)

    # In this case, nothing to join on, so it should just be a copy of sales
    pd.testing.assert_frame_equal(sales_df, enriched)
