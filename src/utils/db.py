import psycopg
from utils.config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def get_db_connection():
    db = config["database"]
    try:
        conn = psycopg.connect(
            host=db["host"],
            port=db["port"],
            dbname=db["name"],
            user=db["user"],
            password=db["password"],
        )
        logger.info("Successfully connected to database")
        return conn
    except Exception as e:
        logger.exception("Exception occurred while connecting to database")
        return None
