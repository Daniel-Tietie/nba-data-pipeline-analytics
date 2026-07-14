"""
Builds mvp_season_profiles by joining mvp_winners, raw_player_season_stats,
raw_season_standings, and raw_league_season_averages. Truncates and rebuilds
on every run.

ts_pct (true shooting %) isn't returned directly by the stats API, so it's
computed here from per-game FGA/FTA/PTS: PTS / (2 * (FGA + 0.44 * FTA)).
league_avg_ts_pct is the same formula applied to the league's average
FGA/FTA/PTS for that season (an approximation of the average player's TS%,
not the average of individual TS% values).
"""

import logging
import sys
import os
from typing import Any, Dict, List, Optional

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


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        dbname=os.getenv('DB_NAME', 'nba_pipeline'),
        user=os.getenv('DB_USER', 'nba_user'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT', '5432')
    )


def true_shooting_pct(pts: Optional[float], fga: Optional[float], fta: Optional[float]) -> Optional[float]:
    if pts is None or fga is None or fta is None:
        return None
    pts, fga, fta = float(pts), float(fga), float(fta)
    denom = 2 * (fga + 0.44 * fta)
    return round(pts / denom, 3) if denom else None


def fetch_mvp_stats(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                w.season, w.player_id, w.player_name, w.team_id,
                s.points_per_game, s.rebounds_per_game, s.assists_per_game,
                s.fg_pct, s.three_pt_pct, s.player_efficiency_rating, s.win_shares,
                s.raw_data
            FROM mvp_winners w
            JOIN raw_player_season_stats s
                ON s.player_id = w.player_id AND s.season = w.season
            ORDER BY w.season
        """)
        cols = [
            'season', 'player_id', 'player_name', 'team_id',
            'points_per_game', 'rebounds_per_game', 'assists_per_game',
            'fg_pct', 'three_pt_pct', 'player_efficiency_rating', 'win_shares',
            'raw_data',
        ]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_standings_lookup(conn) -> Dict[Any, Dict[str, Any]]:
    """(season, team_id) -> {wins, win_pct, conference_rank}."""
    with conn.cursor() as cur:
        cur.execute("SELECT season, team_id, wins, win_pct, conference_rank FROM raw_season_standings")
        return {
            (season, team_id): {'wins': wins, 'win_pct': win_pct, 'conference_rank': rank}
            for season, team_id, wins, win_pct, rank in cur.fetchall()
        }


def fetch_league_averages(conn) -> Dict[str, Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT season, avg_pts, avg_reb, avg_ast, avg_fga, avg_fta FROM raw_league_season_averages")
        return {
            season: {'avg_pts': pts, 'avg_reb': reb, 'avg_ast': ast, 'avg_fga': fga, 'avg_fta': fta}
            for season, pts, reb, ast, fga, fta in cur.fetchall()
        }


def build() -> Dict[str, Any]:
    """Truncate and rebuild mvp_season_profiles."""
    conn = get_db_conn()
    try:
        mvp_stats = fetch_mvp_stats(conn)
        if not mvp_stats:
            logger.warning("No MVP stats found, nothing to build")
            return {'rows_built': 0}

        standings = fetch_standings_lookup(conn)
        league_avgs = fetch_league_averages(conn)

        records = []
        missing_context = []

        for row in mvp_stats:
            season, player_id, team_id = row['season'], row['player_id'], row['team_id']
            raw = row['raw_data'] or {}

            ts_pct = true_shooting_pct(row['points_per_game'], raw.get('FGA'), raw.get('FTA'))

            team_info = standings.get((season, team_id))
            if not team_info:
                missing_context.append(season)

            league = league_avgs.get(season)
            league_avg_ts_pct = None
            if league:
                league_avg_ts_pct = true_shooting_pct(league['avg_pts'], league['avg_fga'], league['avg_fta'])

            records.append((
                season, player_id, row['player_name'],
                row['points_per_game'], row['rebounds_per_game'], row['assists_per_game'],
                row['fg_pct'], row['three_pt_pct'], ts_pct,
                row['player_efficiency_rating'], row['win_shares'],
                team_info['wins'] if team_info else None,
                team_info['conference_rank'] if team_info else None,
                team_info['win_pct'] if team_info else None,
                league['avg_pts'] if league else None,
                league['avg_reb'] if league else None,
                league['avg_ast'] if league else None,
                league_avg_ts_pct,
            ))

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE mvp_season_profiles")
            execute_values(cur, """
                INSERT INTO mvp_season_profiles (
                    season, player_id, player_name,
                    points_per_game, rebounds_per_game, assists_per_game,
                    fg_pct, three_pt_pct, ts_pct,
                    player_efficiency_rating, win_shares,
                    team_wins, team_seed, team_win_pct,
                    league_avg_pts, league_avg_reb, league_avg_ast, league_avg_ts_pct
                ) VALUES %s
            """, records)
        conn.commit()

        logger.info(f"Built {len(records)} mvp_season_profiles rows")
        if missing_context:
            logger.warning(f"Seasons missing standings context: {missing_context}")

        run_quality_checks(conn, records, expected=10)
        return {'rows_built': len(records)}
    finally:
        conn.close()


def run_quality_checks(conn, records: List[tuple], expected: int) -> None:
    total = len(records)

    log_check(
        conn, 'row_count_matches_expected', 'mvp_season_profiles',
        passed=(total == expected), records_checked=total, records_failed=abs(expected - total),
        details={'expected': expected, 'actual': total},
        error_message=None if total == expected else f"expected {expected} rows, built {total}",
    )

    bad_ts = sum(1 for r in records if r[8] is not None and not (0 <= r[8] <= 1))
    log_check(
        conn, 'ts_pct_in_range', 'mvp_season_profiles',
        passed=(bad_ts == 0), records_checked=total, records_failed=bad_ts,
        details={'rule': 'ts_pct between 0 and 1'},
        error_message=None if bad_ts == 0 else f"{bad_ts} rows with ts_pct outside [0,1]",
    )

    null_keys = sum(1 for r in records if r[0] is None or r[1] is None or r[3] is None)
    log_check(
        conn, 'key_columns_not_null', 'mvp_season_profiles',
        passed=(null_keys == 0), records_checked=total, records_failed=null_keys,
        details={'rule': 'season, player_id, points_per_game not null'},
        error_message=None if null_keys == 0 else f"{null_keys} rows with a null key column",
    )


if __name__ == '__main__':
    result = build()
    print(f"mvp_season_profiles: {result['rows_built']} rows")
