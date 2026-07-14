"""
Builds player_shooting_zones from raw_shot_zone_splits.
Truncates and rebuilds on every run. Only players above MIN_SEASON_FGA total
attempts are included, so the dashboard dropdown isn't full of players who
barely played. League-average FG% and attempt share per zone are computed
from every player in the raw table, not just the qualifying ones.
"""

import logging
import sys
import os
from collections import defaultdict
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from .quality import log_check

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

MIN_SEASON_FGA = 200


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        dbname=os.getenv('DB_NAME', 'nba_pipeline'),
        user=os.getenv('DB_USER', 'nba_user'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT', '5432')
    )


def fetch_raw_splits(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT player_id, player_name, season, zone, fga, fgm, fg_pct
            FROM raw_shot_zone_splits
        """)
        cols = ['player_id', 'player_name', 'season', 'zone', 'fga', 'fgm', 'fg_pct']
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def compute_league_zone_stats(rows: List[Dict]) -> Dict:
    """(season, zone) -> {fg_pct, attempt_share} across every player, unfiltered."""
    zone_totals = defaultdict(lambda: {'fga': 0, 'fgm': 0})
    season_totals = defaultdict(int)

    for r in rows:
        zone_totals[(r['season'], r['zone'])]['fga'] += r['fga'] or 0
        zone_totals[(r['season'], r['zone'])]['fgm'] += r['fgm'] or 0
        season_totals[r['season']] += r['fga'] or 0

    league = {}
    for (season, zone), t in zone_totals.items():
        league[(season, zone)] = {
            'fg_pct': round(t['fgm'] / t['fga'], 3) if t['fga'] else None,
            'attempt_share': round(t['fga'] / season_totals[season], 3) if season_totals[season] else None,
        }
    return league


def build() -> Dict[str, Any]:
    """Truncate and rebuild player_shooting_zones from raw_shot_zone_splits."""
    conn = get_db_conn()
    try:
        rows = fetch_raw_splits(conn)
        if not rows:
            logger.warning("No raw shot zone data found, nothing to build")
            return {'rows_built': 0}

        league_zone_stats = compute_league_zone_stats(rows)

        player_total_fga = defaultdict(int)
        for r in rows:
            player_total_fga[(r['player_id'], r['season'])] += r['fga'] or 0

        qualifying = [
            r for r in rows
            if player_total_fga[(r['player_id'], r['season'])] >= MIN_SEASON_FGA
        ]

        records = []
        for r in qualifying:
            total_fga = player_total_fga[(r['player_id'], r['season'])]
            league = league_zone_stats.get((r['season'], r['zone']), {})
            records.append((
                r['player_id'], r['player_name'], r['season'], r['zone'],
                r['fga'], r['fgm'], r['fg_pct'],
                round(r['fga'] / total_fga, 3) if total_fga else None,
                league.get('fg_pct'), league.get('attempt_share'),
            ))

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE player_shooting_zones")
            execute_values(cur, """
                INSERT INTO player_shooting_zones (
                    player_id, player_name, season, zone, fga, fgm, fg_pct,
                    zone_frequency, league_fg_pct, league_attempt_share
                ) VALUES %s
            """, records)
        conn.commit()

        qualifying_players = {(r['player_id'], r['season']) for r in qualifying}
        logger.info(
            f"Built {len(records)} player_shooting_zones rows "
            f"({len(qualifying_players)} player-seasons above {MIN_SEASON_FGA} FGA)"
        )

        run_quality_checks(conn, records)
        return {'rows_built': len(records)}
    finally:
        conn.close()


def run_quality_checks(conn, records: List[tuple]) -> None:
    total = len(records)

    bad_pct = sum(1 for r in records if r[6] is not None and not (0 <= r[6] <= 1))
    log_check(
        conn, 'fg_pct_in_range', 'player_shooting_zones',
        passed=(bad_pct == 0), records_checked=total, records_failed=bad_pct,
        details={'rule': 'fg_pct between 0 and 1'},
        error_message=None if bad_pct == 0 else f"{bad_pct} rows with fg_pct outside [0,1]",
    )

    null_keys = sum(1 for r in records if r[0] is None or r[2] is None or r[3] is None)
    log_check(
        conn, 'key_columns_not_null', 'player_shooting_zones',
        passed=(null_keys == 0), records_checked=total, records_failed=null_keys,
        details={'rule': 'player_id, season, zone not null'},
        error_message=None if null_keys == 0 else f"{null_keys} rows with a null key column",
    )


if __name__ == '__main__':
    result = build()
    print(f"player_shooting_zones: {result['rows_built']} rows")
