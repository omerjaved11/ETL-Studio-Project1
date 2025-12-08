# src/etl/retail/extract.py
from pathlib import Path
import pandas as pd

from ...utils.logger import get_logger

logger = get_logger(__name__)

# Project root: src/etl/retail -> etl -> src -> project
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_INPUT_DIR = PROJECT_ROOT / "data" / "input"

CUSTOMERS_CSV = DATA_INPUT_DIR / "customers.csv"
TRANSACTIONS_CSV = DATA_INPUT_DIR / "transactions.csv"


def extract_retail(
    customers_path: Path | None = None,
    transactions_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract step for retail ETL:
    Read customers and transactions CSVs into DataFrames.
    """
    customers_path = customers_path or CUSTOMERS_CSV
    transactions_path = transactions_path or TRANSACTIONS_CSV

    if not customers_path.exists():
        raise FileNotFoundError(f"Customers CSV not found at {customers_path}")
    if not transactions_path.exists():
        raise FileNotFoundError(f"Transactions CSV not found at {transactions_path}")

    logger.info("[RETAIL-EXTRACT] Reading customers from %s", customers_path)
    customers_df = pd.read_csv(customers_path)
    logger.info("[RETAIL-EXTRACT] Customers shape: %s", customers_df.shape)

    logger.info("[RETAIL-EXTRACT] Reading transactions from %s", transactions_path)
    transactions_df = pd.read_csv(transactions_path)
    logger.info("[RETAIL-EXTRACT] Transactions shape: %s", transactions_df.shape)

    return customers_df, transactions_df
