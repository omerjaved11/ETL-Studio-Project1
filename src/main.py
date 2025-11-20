from utils.logger import get_logger
from utils.db import get_db_connection

logger = get_logger(__name__)

def run_etl():
    logger.info("Starting ETL")
    conn = get_db_connection()
    logger.info("Stopping ETL")


if __name__ == "__main__":
    run_etl()