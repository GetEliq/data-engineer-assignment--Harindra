from pathlib import Path
from typing import Union
import pandas as pd
from sqlalchemy import create_engine, text
from .logger import get_logger
from ..config import settings

logger = get_logger(__name__, csv_path=settings.LOG_CSV)

def save_to_sqlite_table(df: pd.DataFrame, table_name: str, sqlite_path: Union[str, Path], if_exists: str = "replace"):
    sqlite_path = Path(sqlite_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{sqlite_path}")
    logger.info("Saving %d rows to table %s in %s", len(df), table_name, sqlite_path)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    with engine.connect() as conn:
        try:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dev ON {table_name} (ext_dev_ref)"))
            if "date_local" in df.columns:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date_local)"))
            if "timestamp_local" in df.columns:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts ON {table_name} (timestamp_local)"))
        except Exception as e:
            logger.debug("Index creation issue: %s", e)
    logger.info("Saved table %s", table_name)

def read_sql_to_df(sql: str, sqlite_path: Union[str, Path]) -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{Path(sqlite_path)}")
    return pd.read_sql_query(sql, engine)
