import os
import psycopg2
from dotenv import load_dotenv
from db_setup.logs_config import get_logger

# ---------------------------------------------------
# Load environment variables from .env file
# ---------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH)

# Initialize logger
logger = get_logger(__name__)


def get_connection():
    """Establish a connection to PostgreSQL using environment variables."""
    try:
        # Fetch database configuration strictly from .env
        db_params = {
            "host": os.getenv("DB_HOST"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "port": os.getenv("DB_PORT"),
        }

        # Validate that all required environment variables exist
        missing_vars = [k for k, v in db_params.items() if not v]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

        logger.info(
            f"Attempting PostgreSQL connection on host={db_params['host']}, "
            f"port={db_params['port']}, user={db_params['user']}..."
        )

        # Establish connection
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        logger.info(f"Connected to PostgreSQL database: {db_params['database']} successfully.")
        return conn

    except psycopg2.OperationalError as e:
        logger.error(f"Connection failed: {e}")
        raise ConnectionError("Database connection failed. Please verify host, port, and credentials.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
