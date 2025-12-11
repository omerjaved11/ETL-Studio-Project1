# tests/test_retail_extract.py
from pathlib import Path

import pandas as pd
import pytest

from src.etl.retail.extract import extract_retail


def _write_csv(path: Path, data: dict) -> None:
    df = pd.DataFrame(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def test_extract_retail_with_explicit_paths(tmp_path):
    sales_csv = tmp_path / "sales.csv"
    products_csv = tmp_path / "product_hierarchy.csv"
    stores_csv = tmp_path / "store_cities.csv"

    _write_csv(
        sales_csv,
        {"sale_id": [1, 2], "amount": [10.0, 20.0]},
    )
    _write_csv(
        products_csv,
        {"product_id": [101, 102], "product_name": ["Widget", "Gadget"]},
    )
    _write_csv(
        stores_csv,
        {"store_id": [1, 2], "city": ["NYC", "LA"]},
    )

    sales_df, products_df, stores_df = extract_retail(
        sales_path=sales_csv,
        products_path=products_csv,
        stores_path=stores_csv,
    )

    assert list(sales_df.columns) == ["sale_id", "amount"]
    assert sales_df.shape == (2, 2)

    assert list(products_df.columns) == ["product_id", "product_name"]
    assert products_df.shape == (2, 2)

    assert list(stores_df.columns) == ["store_id", "city"]
    assert stores_df.shape == (2, 2)


def test_extract_retail_missing_sales_raises(tmp_path):
    # Only create products and stores
    products_csv = tmp_path / "product_hierarchy.csv"
    stores_csv = tmp_path / "store_cities.csv"
    _write_csv(products_csv, {"product_id": [1], "name": ["P"]})
    _write_csv(stores_csv, {"store_id": [1], "city": ["X"]})

    sales_csv = tmp_path / "sales.csv"  # do NOT create

    with pytest.raises(FileNotFoundError) as exc:
        extract_retail(
            sales_path=sales_csv,
            products_path=products_csv,
            stores_path=stores_csv,
        )

    assert "sales.csv not found" in str(exc.value)


def test_extract_retail_missing_products_raises(tmp_path):
    sales_csv = tmp_path / "sales.csv"
    stores_csv = tmp_path / "store_cities.csv"
    _write_csv(sales_csv, {"sale_id": [1], "amount": [10.0]})
    _write_csv(stores_csv, {"store_id": [1], "city": ["X"]})

    products_csv = tmp_path / "product_hierarchy.csv"  # do NOT create

    with pytest.raises(FileNotFoundError) as exc:
        extract_retail(
            sales_path=sales_csv,
            products_path=products_csv,
            stores_path=stores_csv,
        )

    assert "product_hierarchy.csv not found" in str(exc.value)


def test_extract_retail_missing_stores_raises(tmp_path):
    sales_csv = tmp_path / "sales.csv"
    products_csv = tmp_path / "product_hierarchy.csv"
    _write_csv(sales_csv, {"sale_id": [1], "amount": [10.0]})
    _write_csv(products_csv, {"product_id": [1], "name": ["P"]})

    stores_csv = tmp_path / "store_cities.csv"  # do NOT create

    with pytest.raises(FileNotFoundError) as exc:
        extract_retail(
            sales_path=sales_csv,
            products_path=products_csv,
            stores_path=stores_csv,
        )

    assert "store_cities.csv not found" in str(exc.value)
