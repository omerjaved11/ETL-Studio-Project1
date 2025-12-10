# src/etl/retail_/run.py
from __future__ import annotations
from dataclasses import dataclass

from ...utils.logger import get_logger
from .extract import extract_retail
from .transform import (
    transform_sales,
    transform_products,
    transform_stores,
    join_sales_products_stores,
)
from .load import load_retail_to_db

logger = get_logger(__name__)


@dataclass
class RetailETLResult:
    sales_raw_shape: tuple[int, int]
    products_raw_shape: tuple[int, int]
    stores_raw_shape: tuple[int, int]
    sales_clean_shape: tuple[int, int]
    products_clean_shape: tuple[int, int]
    stores_clean_shape: tuple[int, int]
    enriched_shape: tuple[int, int]


def run_retail_etl(batch_size: int = 1000) -> RetailETLResult:
    """
    Run full ETL for  Alan's retail dataset:
      - Extract: sales, products, stores
      - Transform: clean each
      - Join: sales + products + stores
      - Load: 4 tables into Postgres
    """
    logger.info(
        "[-RUN] Starting  retail ETL pipeline (batch_size=%d)",
        batch_size,
    )

    # Extract
    sales_raw, products_raw, stores_raw = extract_retail()
    sales_raw_shape = sales_raw.shape
    products_raw_shape = products_raw.shape
    stores_raw_shape = stores_raw.shape

    # Transform
    sales_clean = transform_sales(sales_raw)
    products_clean = transform_products(products_raw)
    stores_clean = transform_stores(stores_raw)
    enriched = join_sales_products_stores(sales_clean, products_clean, stores_clean)

    sales_clean_shape = sales_clean.shape
    products_clean_shape = products_clean.shape
    stores_clean_shape = stores_clean.shape
    enriched_shape = enriched.shape

    # Load
    load_retail_to_db(
        sales_clean=sales_clean,
        products_clean=products_clean,
        stores_clean=stores_clean,
        enriched=enriched,
        batch_size=batch_size,
    )

    logger.info("[Retail-RUN]  retail ETL pipeline completed successfully")

    return RetailETLResult(
        sales_raw_shape=sales_raw_shape,
        products_raw_shape=products_raw_shape,
        stores_raw_shape=stores_raw_shape,
        sales_clean_shape=sales_clean_shape,
        products_clean_shape=products_clean_shape,
        stores_clean_shape=stores_clean_shape,
        enriched_shape=enriched_shape,
    )
