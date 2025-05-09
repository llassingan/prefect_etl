from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from ..utils.config import settings 
def get_connection_postgres() -> Connection:
    """
    Create and return a PostgreSQL connection to a specific schema using credentials from .env
    """
    db_url = (
        f"postgresql+psycopg2://{settings.DB_USER}:{settings.DB_PASS}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    # engine = create_engine(db_url, connect_args={"options": f"-c search_path={settings.DB_SCHEMA}"})
    engine = create_engine(db_url)
    return engine.connect()
