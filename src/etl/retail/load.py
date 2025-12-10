import pandas as pd

from ...utils.logger import get_logger
from ...utils.db import load_dataframe_to_table

logger = get_logger(__name__)


def load_retail_to_db(
    sales_clean: pd.DataFrame,
    products_clean: pd.DataFrame,
    stores_clean: pd.DataFrame,
    enriched: pd.DataFrame,
    batch_size: int = 1000,
) -> None:
    """
    Load cleaned tables + enriched join into PostgreSQL.

    For now we still use load_dataframe_to_table in 'overwrite' mode.
    batch_size is logged for future optimization.
    """
    logger.info(
        "[RETAIL-LOAD] Starting load for retail pipeline (batch_size=%d)",
        batch_size,
    )

    load_dataframe_to_table(products_clean, "retail_products_clean", mode="overwrite")
    load_dataframe_to_table(stores_clean, "retail_stores_clean", mode="overwrite")
    load_dataframe_to_table(sales_clean, "retail_sales_clean", mode="overwrite")
    load_dataframe_to_table(enriched, "retail_sales_enriched", mode="overwrite")

    logger.info(
        "[-LOAD] Loaded products(%d rows), stores(%d rows), sales(%d rows), enriched(%d rows)",
        len(products_clean), len(stores_clean), len(sales_clean), len(enriched),
    )
    logger.info(
        "[-LOAD] NOTE: batch_size=%d is currently only logged. "
        "Later we can implement chunked inserts using this size.",
        batch_size,
    )
