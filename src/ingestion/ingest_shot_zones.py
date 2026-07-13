"""
Ingestion script for NBA player shot zone splits.
Fetches league-wide shot location splits per season via stats_client's
leaguedashplayershotlocations call. One request per season covers every
player, avoiding per-player shotchartdetail calls, which is what triggered
the June API block.
"""

import logging
import sys
import math
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from dotenv import load_dotenv
from typing import Any, Dict, List

from .stats_client import fetch_raw, league_dash_params

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

SEASONS = ['2021-22', '2022-23', '2023-24']

# leaguedashplayershotlocations groups FGM/FGA/FG_PCT triples under 8 zone
# headers, in this fixed order. Backcourt and the combined Corner 3 total
# are dropped; the dashboard only needs the 6 court zones.
ZONES = [
    'Restricted Area', 'In The Paint (Non-RA)', 'Mid-Range',
    'Left Corner 3', 'Right Corner 3', 'Above the Break 3',
]

# PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, AGE, NICKNAME
BASE_COLUMNS = 6


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


def fetch_shot_zones_for_season(season: str) -> List[Dict[str, Any]]:
    """Fetch per-zone shot splits for every player in a season."""
    logger.info(f"Fetching shot zone splits for {season} ...")

    params = league_dash_params(season, "Regular Season", "Totals", DistanceRange="By Zone")
    payload = fetch_raw("leaguedashplayershotlocations", params)
    if payload is None:
        logger.warning(f"Empty shot zone response for {season}")
        return []

    row_set = payload["resultSets"]["rowSet"]

    records = []
    for row in row_set:
        player_id = int(row[0])
        player_name = str(row[1])
        for i, zone in enumerate(ZONES):
            offset = BASE_COLUMNS + i * 3
            fgm, fga, fg_pct = row[offset], row[offset + 1], row[offset + 2]
            records.append({
                'player_id': player_id,
                'player_name': player_name,
                'season': season,
                'zone': zone,
                'fga': clean_value(fga),
                'fgm': clean_value(fgm),
                'fg_pct': clean_value(fg_pct),
                'raw_data': {'row': [clean_value(v) for v in row]},
            })

    logger.info(f"  -> {len(records)} player-zone rows fetched for {season}")
    return records


def insert_shot_zones(records: List[Dict[str, Any]], conn) -> int:
    """Upsert shot zone records into raw_shot_zone_splits."""
    if not records:
        return 0

    query = """
        INSERT INTO raw_shot_zone_splits (
            player_id, player_name, season, zone, fga, fgm, fg_pct, raw_data
        ) VALUES %s
        ON CONFLICT (player_id, season, zone) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            fga         = EXCLUDED.fga,
            fgm         = EXCLUDED.fgm,
            fg_pct      = EXCLUDED.fg_pct,
            raw_data    = EXCLUDED.raw_data,
            ingested_at = CURRENT_TIMESTAMP
    """

    values = [
        (
            r['player_id'], r['player_name'], r['season'], r['zone'],
            r['fga'], r['fgm'], r['fg_pct'], Json(r['raw_data'])
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    return len(values)


def run(seasons: List[str] = None) -> Dict[str, Any]:
    """Fetch and store shot zone splits for each season."""
    seasons = seasons or SEASONS
    conn = get_db_conn()

    total_inserted = 0
    results = {}
    skipped = []

    try:
        for season in seasons:
            records = fetch_shot_zones_for_season(season)

            if records:
                inserted = insert_shot_zones(records, conn)
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
    print("\n===== SHOT ZONE INGESTION SUMMARY =====")
    for season, info in summary['by_season'].items():
        status = f"{info['rows']} player-zone rows" if info['rows'] > 0 else "SKIPPED"
        print(f"  {season}: {status}")
    print(f"\n  TOTAL ROWS: {summary['total']}")
    if summary['skipped']:
        print(f"  NEEDS RETRY: {summary['skipped']}")
    print("=========================================")
