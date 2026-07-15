"""
Builds playoff_upsets from raw_season_standings and raw_playoff_games.
Truncates and rebuilds on every run.

"Upset" here means the #1 seed failed to reach the conference finals --
lost in round 1 or lost in round 2 (conference semifinals). Winning both
rounds counts as success (upset=False) regardless of what happens in the
conference finals itself. For each season/conference, the row's opponent
is whichever team ended the 1-seed's run (round 1 or round 2 loss), or
their round-2 opponent if they advanced to the conference finals.

Round-1 opponent seed is hardcoded to 8, not looked up from
raw_season_standings.conference_rank: that field doesn't get renumbered
after the play-in tournament decides the final 7/8 slots, so in play-in
seasons it can show the pre-play-in record-based rank instead of the real
bracket seed (verified against 2022-23: Miami shows conference_rank 7 but
actually played as the bracket 8-seed against Milwaukee, having lost the
7-vs-8 play-in game to Atlanta, who took the 7 slot and played Boston).
Round 1 is always a straight 1-vs-8 pairing by construction, so no lookup
is needed there. Round-2 opponents (always the winner of the 4-vs-5 series)
aren't affected by that play-in quirk, so their seed is looked up normally.
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


def fetch_seed_lookup(conn) -> Dict[Any, int]:
    """(season, team_id) -> conference_rank, for round-2 opponents only."""
    with conn.cursor() as cur:
        cur.execute("SELECT season, team_id, conference_rank FROM raw_season_standings")
        return {(season, team_id): rank for season, team_id, rank in cur.fetchall()}


def fetch_playoff_rounds(conn, season: str, team_id: int) -> List[Dict[str, Any]]:
    """
    A team's playoff games that season, grouped into chronological rounds
    by opponent (each opponent change starts a new round). Returns up to
    however many rounds they played, in order.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT opponent_team_id, win_loss, game_date
            FROM raw_playoff_games
            WHERE season = %s AND team_id = %s
            ORDER BY game_date
        """, (season, team_id))
        rows = cur.fetchall()

    rounds: List[Dict[str, Any]] = []
    current_opponent = None
    for opponent_id, win_loss, _ in rows:
        if opponent_id != current_opponent:
            rounds.append({'opponent_team_id': opponent_id, 'wins': 0, 'losses': 0})
            current_opponent = opponent_id
        if win_loss == 'W':
            rounds[-1]['wins'] += 1
        else:
            rounds[-1]['losses'] += 1
    return rounds


def resolve_outcome(season: str, team_id: int, rounds: List[Dict[str, Any]],
                     seed_lookup: Dict[Any, int]) -> Optional[Dict[str, Any]]:
    """
    Decide the 1-seed's outcome for the season: which round ended their run
    (if they didn't reach the conference finals), or their round-2 opponent
    if they did.
    """
    if not rounds:
        return None

    round1 = rounds[0]
    if round1['wins'] <= round1['losses']:
        return {
            'stage': 'Round 1', 'reached_cf': False,
            'opponent_team_id': round1['opponent_team_id'],
            'opponent_rank': 8,
            'wins': round1['wins'], 'losses': round1['losses'],
        }

    if len(rounds) < 2:
        logger.warning(f"{season} team {team_id} won round 1 but has no round-2 games logged")
        return None

    round2 = rounds[1]
    reached_cf = round2['wins'] > round2['losses']
    return {
        'stage': 'Conference Finals' if reached_cf else 'Round 2',
        'reached_cf': reached_cf,
        'opponent_team_id': round2['opponent_team_id'],
        'opponent_rank': seed_lookup.get((season, round2['opponent_team_id'])),
        'wins': round2['wins'], 'losses': round2['losses'],
    }


def build() -> Dict[str, Any]:
    """Truncate and rebuild playoff_upsets from standings and playoff game logs."""
    conn = get_db_conn()
    try:
        top_seeds = fetch_top_seeds(conn)
        seed_lookup = fetch_seed_lookup(conn)

        run_top_seed_check(conn, top_seeds)

        records = []
        missing = []

        for seed in top_seeds:
            season, team_id = seed['season'], seed['team_id']
            rounds = fetch_playoff_rounds(conn, season, team_id)
            outcome = resolve_outcome(season, team_id, rounds, seed_lookup)
            if outcome is None:
                missing.append((season, seed['conference']))
                continue

            opponent_rank = outcome['opponent_rank']
            upset_margin = 0
            if not outcome['reached_cf'] and opponent_rank:
                upset_margin = opponent_rank - 1

            records.append((
                season, outcome['stage'], team_id, 1,
                outcome['opponent_team_id'], opponent_rank,
                f"{outcome['wins']}-{outcome['losses']}",
                upset_margin,
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
        if missing:
            logger.warning(f"1-seeds with unresolved playoff outcomes: {missing}")

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
