"""
Ingestion script for NBA playoff game logs.
Fetches team-perspective playoff game logs for the 10 MVP seasons from the
NBA stats API via stats_client, one row per team per game. The ETL layer
derives round-1 series and upsets from this against raw_season_standings.
"""

import logging
import sys
import math
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from dotenv import load_dotenv
from typing import Dict, List, Any

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


def get_team_abbr_map(conn) -> Dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT team_abbr, team_id FROM teams")
        return {abbr: team_id for abbr, team_id in cur.fetchall()}


def parse_opponent_abbr(matchup: str) -> str:
    """'DEN vs. LAL' (home) or 'DEN @ LAL' (away) -> 'LAL'"""
    sep = ' vs. ' if ' vs. ' in matchup else ' @ '
    return matchup.split(sep)[1].strip()


def fetch_playoff_games_for_season(season: str, team_abbr_map: Dict[str, int]) -> List[Dict[str, Any]]:
    """Fetch playoff game logs for a season via leaguegamelog."""
    logger.info(f"Fetching playoff games for {season} ...")

    df = fetch_stats("leaguegamelog", {
        "LeagueID": "00", "Season": season, "SeasonType": "Playoffs",
        "PlayerOrTeam": "T", "Counter": "0", "Sorter": "DATE",
        "Direction": "ASC", "DateFrom": "", "DateTo": "",
    })
    if df is None or df.empty:
        logger.warning(f"Empty playoff game log for {season}")
        return []

    records = []
    for _, row in df.iterrows():
        matchup = str(row['MATCHUP'])
        opponent_abbr = parse_opponent_abbr(matchup)
        raw = {k: clean_value(v) for k, v in row.to_dict().items()}

        records.append({
            'game_id': str(row['GAME_ID']),
            'season': season,
            'game_date': row['GAME_DATE'],
            'team_id': int(row['TEAM_ID']),
            'opponent_team_id': team_abbr_map.get(opponent_abbr),
            'matchup': matchup,
            'win_loss': str(row['WL']),
            'pts': int(row['PTS']) if row.get('PTS') is not None else None,
            'raw_data': raw,
        })

    logger.info(f"  -> {len(records)} team-games fetched for {season}")
    return records


def insert_playoff_games(records: List[Dict[str, Any]], conn) -> int:
    """Upsert playoff game records into raw_playoff_games."""
    if not records:
        return 0

    query = """
        INSERT INTO raw_playoff_games (
            game_id, season, game_date, team_id, opponent_team_id,
            matchup, win_loss, pts, raw_data
        ) VALUES %s
        ON CONFLICT (game_id, team_id) DO UPDATE SET
            opponent_team_id = EXCLUDED.opponent_team_id,
            matchup          = EXCLUDED.matchup,
            win_loss         = EXCLUDED.win_loss,
            pts              = EXCLUDED.pts,
            raw_data         = EXCLUDED.raw_data,
            ingested_at      = CURRENT_TIMESTAMP
    """

    values = [
        (
            r['game_id'], r['season'], r['game_date'], r['team_id'],
            r['opponent_team_id'], r['matchup'], r['win_loss'], r['pts'],
            Json(r['raw_data'])
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    return len(values)


def run(seasons: List[str] = None) -> Dict[str, Any]:
    """Fetch and store playoff game logs for each season."""
    seasons = seasons or SEASONS
    conn = get_db_conn()
    team_abbr_map = get_team_abbr_map(conn)

    total_inserted = 0
    results = {}
    skipped = []

    try:
        for season in seasons:
            records = fetch_playoff_games_for_season(season, team_abbr_map)

            if records:
                inserted = insert_playoff_games(records, conn)
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
    print("\n===== PLAYOFF GAMES INGESTION SUMMARY =====")
    for season, info in summary['by_season'].items():
        status = f"{info['rows']} team-games" if info['rows'] > 0 else "SKIPPED"
        print(f"  {season}: {status}")
    print(f"\n  TOTAL ROWS: {summary['total']}")
    if summary['skipped']:
        print(f"  NEEDS RETRY: {summary['skipped']}")
    print("=============================================")
