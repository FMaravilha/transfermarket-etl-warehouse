import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")

COLS = {
    "clubs.csv": {"club_id", "name"},
    "player_valuations.csv": {"player_id", "date", "market_value_in_eur"},
    "players.csv": {"player_id", "name"},
}


def validate_file(path, required_columns):

    path = Path(path)

    logger.info(f"Validating file: {path.name}")

    if not path.exists():
        logger.error(f"{path.name} is missing")
        raise FileNotFoundError(f"{path.name} is missing")

    df = pd.read_csv(path)

    logger.info(f"{path.name} loaded successfully")
    logger.info(f"{path.name} row count: {len(df)}")

    if df.empty:
        logger.error(f"{path.name} is empty")
        raise ValueError(f"{path.name} is empty")

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        logger.error(f"{path.name} missing columns: {missing_columns}")
        raise ValueError(f"{path.name} missing required columns")

    logger.info(f"{path.name} passed validation")


def main():

    logger.info("Starting RAW validation")

    for filename, columns in COLS.items():
        path = RAW_DIR / filename
        validate_file(path, columns)

    logger.info("All RAW validation tests passed successfully")

if __name__ == "__main__":
    main()