"""
Builds playoff_upsets from raw_season_standings and raw_playoff_games.
Truncates and rebuilds on every run.

For each season and conference, finds the 1-seed and its round-1 opponent
(the opponent of the 1-seed's earliest playoff game that season), tallies
the series, and flags an upset when the 1-seed lost it.

The opponent's seed is hardcoded to 8, not looked up from
raw_season_standings.conference_rank: that field doesn't get renumbered
after the play-in tournament decides the final 7/8 slots, so in play-in
seasons it can show the pre-play-in record-based rank instead of the real
bracket seed (verified against 2022-23: Miami shows conference_rank 7 but
actually played as the bracket 8-seed against Milwaukee, having lost the
7-vs-8 play-in game to Atlanta, who took the 7 slot and played Boston).
Round 1 in every one of these seasons is always a straight 1-vs-8 pairing
by construction, so whoever the 1-seed played in round 1 IS seed 8 --
no lookup needed.
"""

import logging
import sys
import os
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


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        dbname=os.getenv('DB_NAME', 'nba_pipeline'),
        user=os.getenv('DB_USER', 'nba_user'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT', '5432')
    )


def fetch_top_seeds(conn) -> List[Dict[str, Any]]:
    """One row per (season, conference) with conference_rank = 1."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT season, team_id, conference
            FROM raw_season_standings
            WHERE conference_rank = 1
            ORDER BY season, conference
        """)
        return [
            {'season': row[0], 'team_id': row[1], 'conference': row[2]}
            for row in cur.fetchall()
        ]


def fetch_round1_series(conn, season: str, team_id: int) -> List[Dict[str, Any]]:
    """
    Games for a team's round-1 series: the opponent of its earliest playoff
    game that season, and every game against that same opponent.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT opponent_team_id, win_loss, game_date
            FROM raw_playoff_games
            WHERE season = %s AND team_id = %s
            ORDER BY game_date
        """, (season, team_id))
        rows = cur.fetchall()

    if not rows:
        return []

    round1_opponent = rows[0][0]
    return [
        {'opponent_team_id': opp, 'win_loss': wl}
        for opp, wl, _ in rows
        if opp == round1_opponent
    ]


def build() -> Dict[str, Any]:
    """Truncate and rebuild playoff_upsets from standings and playoff game logs."""
    conn = get_db_conn()
    try:
        top_seeds = fetch_top_seeds(conn)

        run_top_seed_check(conn, top_seeds)

        records = []
        missing_series = []

        for seed in top_seeds:
            season, team_id = seed['season'], seed['team_id']
            series = fetch_round1_series(conn, season, team_id)
            if not series:
                missing_series.append((season, seed['conference']))
                continue

            opponent_id = series[0]['opponent_team_id']
            wins = sum(1 for g in series if g['win_loss'] == 'W')
            losses = sum(1 for g in series if g['win_loss'] == 'L')
            is_upset = wins < losses

            records.append((
                season, 'Round 1', team_id, 1,
                opponent_id, 8,
                f"{wins}-{losses}",
                7 if is_upset else 0,
            ))

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE playoff_upsets")
            execute_values(cur, """
                INSERT INTO playoff_upsets (
                    season, round, higher_seed_team_id, higher_seed_rank,
                    lower_seed_team_id, lower_seed_rank, series_result, upset_margin
                ) VALUES %s
            """, records)
        conn.commit()

        logger.info(f"Built {len(records)} playoff_upsets rows")
        if missing_series:
            logger.warning(f"1-seeds with no playoff games found: {missing_series}")

        run_quality_checks(conn, records, expected=len(top_seeds))
        return {'rows_built': len(records)}
    finally:
        conn.close()


def run_top_seed_check(conn, top_seeds: List[Dict[str, Any]]) -> None:
    """Exactly one 1-seed per conference per season."""
    counts: Dict[Any, int] = {}
    for seed in top_seeds:
        key = (seed['season'], seed['conference'])
        counts[key] = counts.get(key, 0) + 1

    duplicates = {k: v for k, v in counts.items() if v != 1}
    log_check(
        conn, 'one_top_seed_per_conference', 'playoff_upsets',
        passed=(len(duplicates) == 0), records_checked=len(counts), records_failed=len(duplicates),
        details={'rule': 'exactly one conference_rank=1 team per season/conference'},
        error_message=None if not duplicates else f"bad counts: {duplicates}",
    )


def run_quality_checks(conn, records: List[tuple], expected: int) -> None:
    total = len(records)

    log_check(
        conn, 'row_count_matches_expected', 'playoff_upsets',
        passed=(total == expected), records_checked=total, records_failed=abs(expected - total),
        details={'expected': expected, 'actual': total},
        error_message=None if total == expected else f"expected {expected} rows, built {total}",
    )

    null_opponent = sum(1 for r in records if r[4] is None)
    log_check(
        conn, 'opponent_resolved', 'playoff_upsets',
        passed=(null_opponent == 0), records_checked=total, records_failed=null_opponent,
        details={'rule': 'lower_seed_team_id not null'},
        error_message=None if null_opponent == 0 else f"{null_opponent} rows missing opponent_team_id",
    )


if __name__ == '__main__':
    result = build()
    print(f"playoff_upsets: {result['rows_built']} rows")
