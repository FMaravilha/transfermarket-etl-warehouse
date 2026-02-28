import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")

COLS = {
    "clubs.csv": {"club_id", "name"},
    "player_valuations.csv": {"player_id", "date", "market_value"},
    "players.csv": {"player_id", "name"},
}


def validate_file(filename, required_columns):

    path = RAW_DIR / filename

    logger.info(f"Validating file: {filename}")

    if not path.exists():
        logger.error(f"{filename} is missing")
        raise FileNotFoundError(f"{filename} is missing")

    df = pd.read_csv(path)

    logger.info(f"{filename} loaded successfully")
    logger.info(f"{filename} row count: {len(df)}")

    if df.empty:
        logger.error(f"{filename} is empty")
        raise ValueError(f"{filename} is empty")

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        logger.error(f"{filename} missing columns: {missing_columns}")
        raise ValueError(f"{filename} missing required columns")

    logger.info(f"{filename} passed validation")


def main():

    logger.info("Starting RAW validation")

    for filename, columns in COLS.items():
        validate_file(filename, columns)

    logger.info("All RAW validation tests passed successfully")

if __name__ == "__main__":
    main()