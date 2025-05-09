from prefect.logging import get_run_logger
from ..db.database_conn import get_connection_postgres

logger = get_run_logger()

def with_transaction(func):
    def wrapper(*args, **kwargs):
        conn = get_connection_postgres()
        logger.info(f"[{func.__name__}] DB connection opened")
        try:
            with conn:
                with conn.cursor() as cursor:
                    # Monkey-patch cursor.execute to log SQL
                    original_execute = cursor.execute

                    def logged_execute(query, params=None):
                        logger.info(f"[SQL] {query} | {params}")
                        return original_execute(query, params)

                    cursor.execute = logged_execute

                    result = func(cursor, *args, **kwargs)
                    logger.info(f"[{func.__name__}] Transaction committed")
                    return result
        except Exception as e:
            logger.exception(f"[{func.__name__}] Transaction failed: {e}")
            raise
        finally:
            conn.close()
            logger.info(f"[{func.__name__}] DB connection closed")
    return wrapper
