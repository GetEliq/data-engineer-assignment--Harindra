import logging
import sys
from pathlib import Path
from typing import Union

def get_logger(name: str = "eliq_etl", level: int = logging.INFO, csv_path: Union[str, Path] = None):
    logger = logging.getLogger(name)
    if logger.handlers:
        logger.setLevel(level)
        return logger

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(ch)

    if csv_path:
        csv_path = Path(csv_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(csv_path, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter('%(asctime)s,%(levelname)s,%(name)s,"%(message)s"'))
        logger.addHandler(fh)

    logger.setLevel(level)
    return logger
