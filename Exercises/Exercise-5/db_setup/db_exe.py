from db_setup.logs_config import get_logger

logger = get_logger(__name__)
def execute_sql(cursor, file_path):
    """
    Execute all SQL Statements from the file.
    """
    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()
            cursor.execute(sql_script)
        logger.info(f"Executed Schema from {file_path} successfully.....")
    except Exception as e:
        logger.error(f"Error in the execution of the SQL File {e}...")
        raise