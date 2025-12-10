from pathlib import Path
import pandas as pd

from ...utils.logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_INPUT_DIR = PROJECT_ROOT / "data" / "input"

SALES_CSV = DATA_INPUT_DIR / "sales.csv"
PRODUCTS_CSV = DATA_INPUT_DIR / "product_hierarchy.csv"
STORES_CSV = DATA_INPUT_DIR / "store_cities.csv"


def extract_retail(
    sales_path: Path | None = None,
    products_path: Path | None = None,
    stores_path: Path | None = None,
):
    """
    Extract step for RETAIL Alan's retail dataset.
    Reads sales.csv, product_hierarchy.csv, store_cities.csv into DataFrames.
    """
    sales_path = sales_path or SALES_CSV
    products_path = products_path or PRODUCTS_CSV
    stores_path = stores_path or STORES_CSV

    if not sales_path.exists():
        raise FileNotFoundError(f"sales.csv not found at {sales_path}")
    if not products_path.exists():
        raise FileNotFoundError(f"product_hierarchy.csv not found at {products_path}")
    if not stores_path.exists():
        raise FileNotFoundError(f"store_cities.csv not found at {stores_path}")

    logger.info("[RETAIL-EXTRACT] Reading sales from %s", sales_path)
    sales_df = pd.read_csv(sales_path)
    logger.info("[RETAIL-EXTRACT] sales shape: %s", sales_df.shape)

    logger.info("[RETAIL-EXTRACT] Reading products from %s", products_path)
    products_df = pd.read_csv(products_path)
    logger.info("[RETAIL-EXTRACT] product_hierarchy shape: %s", products_df.shape)

    logger.info("[RETAIL-EXTRACT] Reading stores from %s", stores_path)
    stores_df = pd.read_csv(stores_path)
    logger.info("[RETAIL-EXTRACT] store_cities shape: %s", stores_df.shape)

    return sales_df, products_df, stores_df
