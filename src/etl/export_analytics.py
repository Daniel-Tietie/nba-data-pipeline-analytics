"""
Exports the analytics tables, plus the small lookup tables the dashboard
joins against, from Postgres into a single SQLite file. The dashboard
falls back to this file when it can't reach Postgres (Streamlit Community
Cloud has no route to a local database). Rerun this and commit the
refreshed data/analytics.db whenever the deployed data needs to update.
"""

import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "analytics.db"

TABLES = [
    "player_shooting_zones",
    "playoff_upsets",
    "mvp_season_profiles",
    "mvp_winners",
    "teams",
]


def get_pg_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'nba_user')}:"
        f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'nba_pipeline')}"
    )
    return create_engine(url)


def export() -> Dict[str, int]:
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    pg_engine = get_pg_engine()
    sqlite_conn = sqlite3.connect(OUTPUT_PATH)

    counts = {}
    try:
        for table in TABLES:
            df = pd.read_sql(f"SELECT * FROM {table}", pg_engine)
            df.to_sql(table, sqlite_conn, if_exists="replace", index=False)
            counts[table] = len(df)
            logger.info(f"  {table}: {len(df)} rows")
    finally:
        sqlite_conn.close()

    return counts


if __name__ == '__main__':
    logger.info(f"Exporting to {OUTPUT_PATH} ...")
    result = export()
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\nWrote {OUTPUT_PATH} ({size_kb:.0f} KB)")
    for table, count in result.items():
        print(f"  {table}: {count} rows")
