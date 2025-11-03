import os
import psycopg2
from dotenv import load_dotenv
from db_setup.logs_config import get_logger

# ---------------------------------------------------
# Ensure environment variables always load correctly
# --------------------------------------------------
# Get the absolute path of the project root (Exercise-5)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Load environment variables from the .env file
load_dotenv(dotenv_path=ENV_PATH)

# Initialize logger
logger = get_logger(__name__)


def get_connection():
    """Establish a connection to PostgreSQL using environment variables."""
    db_params = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "test_db"),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", "root"),
        "port": os.getenv("DB_PORT", "5432")  # Default to 5432 for Windows PostgreSQL installations
    }

    try:
        logger.info(
            f"Attempting PostgreSQL connection on host={db_params['host']}, "
            f"port={db_params['port']}, user={db_params['user']}..."
        )
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        logger.info("Connected to PostgreSQL successfully.")
        return conn

    except psycopg2.OperationalError as e:
        logger.error(f"Connection failed: {e}")
        raise ConnectionError("Database connection failed. Please verify host, port, and credentials.")