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


def is_docker():
    """Detect if running inside a Docker container."""
    try:
        with open("/proc/1/cgroup", "rt") as f:
            content = f.read()
            return "docker" in content or "kubepods" in content
    except FileNotFoundError:
        return False


def get_connection():
    """Establish a connection to PostgreSQL using environment variables."""
    try:
        # Base database parameters
        db_params = {
            "host": os.getenv("DB_HOST"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "port": os.getenv("DB_PORT"),
        }

        # Detect if running inside Docker
        inside_docker = is_docker() or os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"

        # Override settings depending on environment
        if inside_docker:
            logger.info("Running inside Docker → using Docker network settings (host=postgres, port=5432)")
            db_params["host"] = db_params.get("host", "postgres")
            db_params["port"] = db_params.get("port", "5432")
        else:
            logger.info("Running locally → switching DB_HOST to localhost and DB_PORT to 5433")
            db_params["host"] = "localhost"
            db_params["port"] = "5433"

        # Log connection attempt
        logger.info(
            f"Attempting PostgreSQL connection on host={db_params['host']}, "
            f"port={db_params['port']}, user={db_params['user']}..."
        )

        # Validate required environment variables
        missing_vars = [k for k, v in db_params.items() if not v]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Attempt connection
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
