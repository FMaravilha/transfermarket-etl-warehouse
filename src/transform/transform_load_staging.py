import pandas as pd
import psycopg2
from pathlib import Path
import os
import io

RAW_DIR = Path("data/raw")

PLAYERS_FILE = RAW_DIR / "players.csv"
CLUBS_FILE = RAW_DIR / "clubs.csv"
PLAYER_VALUATIONS_FILE = RAW_DIR / "player_valuations.csv"

SQL_DIR = Path("/opt/airflow/sql/staging")
CREATE_SCHEMA_TABLES_SQL = SQL_DIR / "creating_staging_tables.sql"

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
}


# -----------------------------
# Helpers
# -----------------------------

def to_int(series):
    return (pd.to_numeric(series, errors="coerce").round().astype("Int64") )

# -----------------------------
# DB
# -----------------------------

def get_connection():

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    return conn, cur


def create_staging_schema_and_tables(conn, cur):

    with open(CREATE_SCHEMA_TABLES_SQL, "r") as f:
        ddl = f.read()

    cur.execute(ddl)
    conn.commit()


def truncate_staging_tables(conn, cur):

    ddl = "TRUNCATE TABLE staging.player_valuations, staging.players, staging.clubs;"
    
    cur.execute(ddl)
    conn.commit()


# -----------------------------
# Transform Players
# -----------------------------

def transform_players(file):

    df_raw = pd.read_csv(file)

    df = df_raw[
        [
            "player_id",
            "name",
            "date_of_birth",
            "position",
            "sub_position",
            "foot",
            "height_in_cm",
            "market_value_in_eur",
            "highest_market_value_in_eur",
            "country_of_birth",
            "city_of_birth",
            "country_of_citizenship",
        ]
    ].copy()

    df = df.replace(["", " ", "nan", "None"], pd.NA)

    text_cols = [
        "name",
        "position",
        "sub_position",
        "foot",
        "country_of_birth",
        "city_of_birth",
        "country_of_citizenship",
    ]

    for col in text_cols:
        df[col] = df[col].astype("string").str.strip()

    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce").dt.date

    df["player_id"] = to_int(df["player_id"])
    df["height_in_cm"] = to_int(df["height_in_cm"])

    df["market_value_in_eur"] = to_int(df["market_value_in_eur"])
    df["highest_market_value_in_eur"] = to_int(df["highest_market_value_in_eur"])

    df = df.dropna(subset=["player_id"])

    df = df.reset_index(drop=True)

    return df


# -----------------------------
# Transform Clubs
# -----------------------------

def transform_clubs(file):

    df_raw = pd.read_csv(file)

    df = df_raw[
        [
            "club_id",
            "name",
            "domestic_competition_id",
            "total_market_value",
            "squad_size",
            "average_age",
            "national_team_players",
            "stadium_name",
            "stadium_seats",
            "net_transfer_record",
            "coach_name",
        ]
    ].copy()

    df = df.replace(["", " ", "nan", "None"], pd.NA)

    text_cols = [
        "name",
        "domestic_competition_id",
        "stadium_name",
        "net_transfer_record",
        "coach_name",
    ]

    for col in text_cols:
        df[col] = df[col].astype("string").str.strip()

    df["club_id"] = to_int(df["club_id"])

    df["total_market_value"] = to_int(df["total_market_value"])
    df["average_age"] = to_int(df["average_age"])

    df["squad_size"] = to_int(df["squad_size"])
    df["national_team_players"] = to_int(df["national_team_players"])
    df["stadium_seats"] = to_int(df["stadium_seats"])

    df = df.dropna(subset=["club_id"])

    df = df.reset_index(drop=True)

    return df


# -----------------------------
# Transform Player Valuations
# -----------------------------

def transform_player_valuations(file):

    df_raw = pd.read_csv(file)

    df = df_raw[
        [
            "player_id",
            "date",
            "market_value_in_eur",
            "current_club_id",
            "player_club_domestic_competition_id",
        ]
    ].copy()

    df = df.rename(columns={"date": "valuation_date"})

    df = df.replace(["", " ", "nan", "None"], pd.NA)

    df["player_club_domestic_competition_id"] = (
        df["player_club_domestic_competition_id"]
        .astype("string")
        .str.strip()
    )

    df["valuation_date"] = pd.to_datetime(df["valuation_date"], errors="coerce").dt.date

    df["player_id"] = to_int(df["player_id"])
    df["current_club_id"] = to_int(df["current_club_id"])

    df["market_value_in_eur"] = to_int(df["market_value_in_eur"])

    df = df.dropna(subset=["player_id", "valuation_date"])

    df = df.reset_index(drop=True)

    return df


# -----------------------------
# Load
# -----------------------------

def copy_dataframe_to_table(conn, cur, df, table_name):

    buffer = io.StringIO()

    df.to_csv(buffer, index=False, header=False, na_rep="")

    buffer.seek(0)

    copy_sql = f"""
    COPY {table_name}
    FROM STDIN
    WITH CSV
    """

    cur.copy_expert(copy_sql, buffer)

    conn.commit()


# -----------------------------
# Main
# -----------------------------

def main():

    pg_conn, pg_cur = get_connection()

    create_staging_schema_and_tables(pg_conn, pg_cur)

    truncate_staging_tables(pg_conn, pg_cur)

    df_players = transform_players(PLAYERS_FILE)
    df_clubs = transform_clubs(CLUBS_FILE)
    df_player_valuations = transform_player_valuations(PLAYER_VALUATIONS_FILE)

    tables = [
        ("staging.players", df_players),
        ("staging.clubs", df_clubs),
        ("staging.player_valuations", df_player_valuations),
    ]

    for table_name, df in tables:
        copy_dataframe_to_table(pg_conn, pg_cur, df, table_name)

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    main()