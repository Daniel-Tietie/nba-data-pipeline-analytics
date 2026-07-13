"""
Ingestion script for NBA season standings.
Fetches conference standings (wins, losses, playoff seed) for the 10 MVP
seasons from the NBA stats API via stats_client.

Uses leaguestandingsv3 for every season rather than deriving rank from the
games table: it returns the league's own PlayoffRank, which accounts for
tiebreakers a simple win_pct sort would get wrong, and later phases need
correct 1-seeds to identify real playoff upsets.
"""

import logging
import sys
import math
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

from .stats_client import fetch_stats

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

SEASONS = [
    '2015-16', '2016-17', '2017-18', '2018-19', '2019-20',
    '2020-21', '2021-22', '2022-23', '2023-24', '2024-25'
]


def clean_value(val):
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        dbname=os.getenv('DB_NAME', 'nba_pipeline'),
        user=os.getenv('DB_USER', 'nba_user'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT', '5432')
    )


def fetch_standings_for_season(season: str) -> List[Dict[str, Any]]:
    """Fetch conference standings for a season via leaguestandingsv3."""
    logger.info(f"Fetching standings for {season} ...")

    df = fetch_stats("leaguestandingsv3", {
        "LeagueID": "00", "Season": season, "SeasonType": "Regular Season",
    })
    if df is None or df.empty:
        logger.warning(f"Empty standings response for {season}")
        return []

    records = []
    for _, row in df.iterrows():
        raw = {k: clean_value(v) for k, v in row.to_dict().items()}
        records.append({
            'season': season,
            'team_id': int(row['TeamID']),
            'conference': str(row['Conference']),
            'conference_rank': int(row['PlayoffRank']),
            'wins': int(row['WINS']),
            'losses': int(row['LOSSES']),
            'win_pct': round(float(row['WinPCT']), 3),
            'games_back': clean_value(float(row['ConferenceGamesBack'])),
            'raw_data': raw,
        })

    logger.info(f"  -> {len(records)} teams fetched for {season}")
    return records


def insert_standings(records: List[Dict[str, Any]], conn) -> int:
    """Upsert standings records into raw_season_standings."""
    if not records:
        return 0

    query = """
        INSERT INTO raw_season_standings (
            season, team_id, conference, conference_rank,
            wins, losses, win_pct, games_back, raw_data
        ) VALUES %s
        ON CONFLICT (season, team_id) DO UPDATE SET
            conference       = EXCLUDED.conference,
            conference_rank  = EXCLUDED.conference_rank,
            wins             = EXCLUDED.wins,
            losses           = EXCLUDED.losses,
            win_pct          = EXCLUDED.win_pct,
            games_back       = EXCLUDED.games_back,
            raw_data         = EXCLUDED.raw_data,
            ingested_at      = CURRENT_TIMESTAMP
    """

    values = [
        (
            r['season'], r['team_id'], r['conference'], r['conference_rank'],
            r['wins'], r['losses'], r['win_pct'], r['games_back'],
            Json(r['raw_data'])
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    return len(values)


def run(seasons: List[str] = None) -> Dict[str, Any]:
    """Fetch and store standings for each season."""
    seasons = seasons or SEASONS
    conn = get_db_conn()

    total_inserted = 0
    results = {}
    skipped = []

    try:
        for season in seasons:
            records = fetch_standings_for_season(season)

            if records:
                inserted = insert_standings(records, conn)
                total_inserted += inserted
                results[season] = {'rows': inserted}
                logger.info(f"  -> Stored {inserted} rows for {season}")
            else:
                skipped.append(season)
                results[season] = {'rows': 0}
                logger.warning(f"  -> Skipped {season} (no data)")
    finally:
        conn.close()

    logger.info(f"Done. Total rows inserted/updated: {total_inserted}")
    if skipped:
        logger.warning(f"Seasons with no data: {skipped}")

    return {'total': total_inserted, 'by_season': results, 'skipped': skipped}


if __name__ == '__main__':
    summary = run()
    print("\n===== STANDINGS INGESTION SUMMARY =====")
    for season, info in summary['by_season'].items():
        status = f"{info['rows']} teams" if info['rows'] > 0 else "SKIPPED"
        print(f"  {season}: {status}")
    print(f"\n  TOTAL ROWS: {summary['total']}")
    if summary['skipped']:
        print(f"  NEEDS RETRY: {summary['skipped']}")
    print("========================================")
