"""
Ingestion script for NBA season standings.
Primary strategy: derive standings from the games table already in the DB.
Fallback strategy: fetch from NBA stats API for seasons not in the games table.

Populates raw_season_standings with W, L, win_pct, conference, and conference_rank.
"""

import logging
import sys
import time
import math
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

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

API_DELAY = 2.0

# Eastern Conference team IDs (static)
EAST_TEAM_IDS = {
    1610612737, 1610612738, 1610612751, 1610612766, 1610612741,
    1610612739, 1610612765, 1610612754, 1610612748, 1610612749,
    1610612752, 1610612753, 1610612755, 1610612761, 1610612764
}


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


def get_seasons_in_games_table(conn) -> set:
    """Return set of seasons that exist in the games table."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT season FROM games")
        return {row[0] for row in cur.fetchall()}


def compute_standings_from_games(season: str, conn) -> List[Dict[str, Any]]:
    """
    Derive standings from the games table for a given season.
    Counts regular season wins/losses per team, assigns conference rank by win_pct.
    """
    logger.info(f"Computing standings from games table for {season} ...")

    query = """
        SELECT
            t.team_id,
            COUNT(*) FILTER (WHERE g.winner_id = t.team_id)  AS wins,
            COUNT(*) FILTER (WHERE g.winner_id != t.team_id) AS losses
        FROM teams t
        JOIN games g
          ON t.team_id IN (g.home_team_id, g.away_team_id)
        WHERE g.season = %s
          AND g.is_playoff = FALSE
          AND g.game_status = 'final'
        GROUP BY t.team_id
        HAVING COUNT(*) > 0
    """

    with conn.cursor() as cur:
        cur.execute(query, (season,))
        rows = cur.fetchall()

    if not rows:
        logger.warning(f"No game data found in DB for {season}")
        return []

    records = []
    for team_id, wins, losses in rows:
        total = (wins or 0) + (losses or 0)
        win_pct = round(wins / total, 3) if total > 0 else 0.0
        conference = 'East' if team_id in EAST_TEAM_IDS else 'West'
        records.append({
            'season': season,
            'team_id': team_id,
            'conference': conference,
            'conference_rank': None,
            'wins': wins,
            'losses': losses,
            'win_pct': win_pct,
            'games_back': None,
            'raw_data': {'source': 'computed_from_games_table', 'season': season}
        })

    # Assign conference rank by win_pct
    for conf in ('East', 'West'):
        conf_teams = sorted(
            [r for r in records if r['conference'] == conf],
            key=lambda r: (r['win_pct'], r['wins'] or 0),
            reverse=True
        )
        for rank, team in enumerate(conf_teams, start=1):
            team['conference_rank'] = rank

    logger.info(f"  -> {len(records)} teams computed from games table for {season}")
    return records


def fetch_standings_from_api(season: str) -> List[Dict[str, Any]]:
    """
    Fetch standings from NBA stats API for seasons not in the games table.
    Uses LeagueDashTeamStats which is the most reliable endpoint.
    """
    from nba_api.stats.endpoints import leaguedashteamstats

    logger.info(f"Fetching standings from API for {season} ...")
    time.sleep(API_DELAY)

    try:
        result = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            season_type_all_star='Regular Season',
            per_mode_detailed='Totals',
            timeout=60
        )
        df = result.get_data_frames()[0]
    except Exception as e:
        logger.error(f"API call failed for {season}: {e}")
        return []

    if df.empty:
        logger.warning(f"Empty API response for {season}")
        return []

    records = []
    for _, row in df.iterrows():
        team_id = int(row['TEAM_ID'])
        wins = int(row['W']) if row.get('W') is not None else None
        losses = int(row['L']) if row.get('L') is not None else None
        win_pct = round(float(row['W_PCT']), 3) if row.get('W_PCT') is not None else None
        conference = 'East' if team_id in EAST_TEAM_IDS else 'West'
        raw = {k: clean_value(v) for k, v in row.to_dict().items()}

        records.append({
            'season': season,
            'team_id': team_id,
            'conference': conference,
            'conference_rank': None,
            'wins': wins,
            'losses': losses,
            'win_pct': win_pct,
            'games_back': None,
            'raw_data': raw
        })

    # Assign conference rank by win_pct
    for conf in ('East', 'West'):
        conf_teams = sorted(
            [r for r in records if r['conference'] == conf],
            key=lambda r: (r['win_pct'] or 0, r['wins'] or 0),
            reverse=True
        )
        for rank, team in enumerate(conf_teams, start=1):
            team['conference_rank'] = rank

    logger.info(f"  -> {len(records)} teams fetched from API for {season}")
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
    """
    Main entry point. For each season:
    - If game data exists in the DB → compute standings from it
    - Otherwise → fetch from NBA stats API
    """
    seasons = seasons or SEASONS
    conn = get_db_conn()

    db_seasons = get_seasons_in_games_table(conn)
    logger.info(f"Seasons available in games table: {sorted(db_seasons)}")

    total_inserted = 0
    results = {}
    skipped = []

    try:
        for season in seasons:
            if season in db_seasons:
                records = compute_standings_from_games(season, conn)
            else:
                records = fetch_standings_from_api(season)

            if records:
                inserted = insert_standings(records, conn)
                total_inserted += inserted
                results[season] = {'rows': inserted, 'source': 'db' if season in db_seasons else 'api'}
                logger.info(f"  -> Stored {inserted} rows for {season}")
            else:
                skipped.append(season)
                results[season] = {'rows': 0, 'source': 'failed'}
                logger.warning(f"  -> Skipped {season} (no data)")
    finally:
        conn.close()

    logger.info(f"Done. Total rows inserted/updated: {total_inserted}")
    if skipped:
        logger.warning(f"Seasons with no data (need API later): {skipped}")

    return {'total': total_inserted, 'by_season': results, 'skipped': skipped}


if __name__ == '__main__':
    summary = run()
    print("\n===== STANDINGS INGESTION SUMMARY =====")
    for season, info in summary['by_season'].items():
        status = f"{info['rows']} teams ({info['source']})" if info['rows'] > 0 else "SKIPPED"
        print(f"  {season}: {status}")
    print(f"\n  TOTAL ROWS: {summary['total']}")
    if summary['skipped']:
        print(f"  NEEDS API:  {summary['skipped']}")
    print("========================================")
