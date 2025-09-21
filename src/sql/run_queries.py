from pathlib import Path
import sqlite3
import pandas as pd
from ..config import settings
from ..utils.logger import get_logger
from .queries import QUERIES

logger = get_logger(__name__, csv_path=settings.LOG_CSV)
OUTPUT_DIR = Path(settings.SQL_OUTPUT_DIR)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run_queries():
    db_path = Path(settings.SQLITE_DB)
    if not db_path.exists():
        logger.error("SQLite DB not found at %s", db_path)
        return

    conn = sqlite3.connect(db_path)

    for name, sql in QUERIES.items():
        try:
            logger.info("Running query: %s", name)
            df = pd.read_sql_query(sql, conn)
            out_path = OUTPUT_DIR / f"{name}.csv"
            df.to_csv(out_path, index=False)
            logger.info("Saved %s to %s (rows=%d)", name, out_path, len(df))
        except Exception as e:
            logger.exception("Query '%s' failed: %s", name, e)

    conn.close()
    logger.info("All queries executed.")
