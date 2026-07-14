"""
Ingestion script for NBA player season stats.
Fetches per-game stats for all 10 MVP seasons from the NBA stats API.
Also pulls team W/L for each season to support MVP profile analysis
(team_wins, team_seed) without needing a separate standings table.

Fetches through stats_client (curl_cffi) — stats.nba.com drops plain
Python requests, which is what made earlier runs hang until timeout.
"""

import logging
import sys
import math
import psycopg2
from psycopg2.extras import execute_values, Json
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

from .stats_client import fetch_stats, league_dash_params

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 10 MVP seasons we care about
MVP_SEASONS = [
    '2015-16', '2016-17', '2017-18', '2018-19', '2019-20',
    '2020-21', '2021-22', '2022-23', '2023-24', '2024-25'
]

# Rotation-level cutoff for league-average context stats. Averaging in deep
# bench players would understate what "league average" means next to an MVP.
MIN_MINUTES_FOR_LEAGUE_AVG = 20.0


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


def get_mvp_players(conn) -> List[Dict]:
    """Load MVP player_id + season from the seeded mvp_winners table."""
    with conn.cursor() as cur:
        cur.execute("SELECT season, player_id, player_name, team_abbr FROM mvp_winners ORDER BY season")
        return [
            {'season': row[0], 'player_id': row[1], 'player_name': row[2], 'team_abbr': row[3]}
            for row in cur.fetchall()
        ]


def fetch_league_player_df(season: str):
    """Fetch the full per-game stats table for every player in a season."""
    logger.info(f"Fetching player stats for {season} ...")
    return fetch_stats(
        "leaguedashplayerstats",
        league_dash_params(season, "Regular Season", "PerGame"),
    )


def build_player_stats_dict(df) -> Dict[int, Dict]:
    """Turn the full-league stats DataFrame into a dict keyed by player_id."""
    stats_by_player = {}
    for _, row in df.iterrows():
        player_id = int(row['PLAYER_ID'])
        raw = {k: clean_value(v) for k, v in row.to_dict().items()}
        stats_by_player[player_id] = {
            'player_name': str(row.get('PLAYER_NAME', '')),
            'team_id': int(row['TEAM_ID']) if row.get('TEAM_ID') else None,
            'games_played': int(row['GP']) if row.get('GP') is not None else None,
            'points_per_game': float(row['PTS']) if row.get('PTS') is not None else None,
            'rebounds_per_game': float(row['REB']) if row.get('REB') is not None else None,
            'assists_per_game': float(row['AST']) if row.get('AST') is not None else None,
            'steals_per_game': float(row['STL']) if row.get('STL') is not None else None,
            'blocks_per_game': float(row['BLK']) if row.get('BLK') is not None else None,
            'fg_pct': float(row['FG_PCT']) if row.get('FG_PCT') is not None else None,
            'three_pt_pct': float(row['FG3_PCT']) if row.get('FG3_PCT') is not None else None,
            'ft_pct': float(row['FT_PCT']) if row.get('FT_PCT') is not None else None,
            'minutes_per_game': float(row['MIN']) if row.get('MIN') is not None else None,
            'player_efficiency_rating': None,  # not in LeagueDashPlayerStats, fetched separately
            'win_shares': None,                # not available via NBA API, left null
            'raw_data': raw
        }

    logger.info(f"  -> {len(stats_by_player)} players parsed")
    return stats_by_player


def compute_league_averages(df, season: str) -> Dict[str, Any]:
    """
    Average per-game stats across rotation-level players (MIN >= threshold),
    for the league-average context columns in mvp_season_profiles.
    """
    qualified = df[df['MIN'] >= MIN_MINUTES_FOR_LEAGUE_AVG]
    if qualified.empty:
        return {}

    return {
        'season': season,
        'qualified_players': len(qualified),
        'avg_pts': round(float(qualified['PTS'].mean()), 1),
        'avg_reb': round(float(qualified['REB'].mean()), 1),
        'avg_ast': round(float(qualified['AST'].mean()), 1),
        'avg_fga': round(float(qualified['FGA'].mean()), 1),
        'avg_fta': round(float(qualified['FTA'].mean()), 1),
        'avg_fg_pct': round(float(qualified['FG_PCT'].mean()), 3),
        'raw_data': {'min_minutes_threshold': MIN_MINUTES_FOR_LEAGUE_AVG},
    }


def insert_league_averages(records: List[Dict], conn) -> int:
    """Upsert records into raw_league_season_averages."""
    if not records:
        return 0

    query = """
        INSERT INTO raw_league_season_averages (
            season, qualified_players, avg_pts, avg_reb, avg_ast,
            avg_fga, avg_fta, avg_fg_pct, raw_data
        ) VALUES %s
        ON CONFLICT (season) DO UPDATE SET
            qualified_players = EXCLUDED.qualified_players,
            avg_pts           = EXCLUDED.avg_pts,
            avg_reb           = EXCLUDED.avg_reb,
            avg_ast           = EXCLUDED.avg_ast,
            avg_fga           = EXCLUDED.avg_fga,
            avg_fta           = EXCLUDED.avg_fta,
            avg_fg_pct        = EXCLUDED.avg_fg_pct,
            raw_data          = EXCLUDED.raw_data,
            ingested_at       = CURRENT_TIMESTAMP
    """

    values = [
        (
            r['season'], r['qualified_players'], r['avg_pts'], r['avg_reb'],
            r['avg_ast'], r['avg_fga'], r['avg_fta'], r['avg_fg_pct'],
            Json(r['raw_data'])
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    return len(values)


def fetch_team_wins_for_season(season: str) -> Dict[int, int]:
    """
    Fetch team W totals for a season using leaguedashteamstats.
    Returns dict keyed by team_id -> wins.
    """
    logger.info(f"Fetching team wins for {season} ...")

    df = fetch_stats(
        "leaguedashteamstats",
        league_dash_params(season, "Regular Season", "Totals"),
    )
    if df is None or df.empty:
        logger.error(f"Team stats fetch failed for {season}")
        return {}

    return {int(row['TEAM_ID']): int(row['W']) for _, row in df.iterrows() if row.get('W') is not None}


def insert_player_season_stats(records: List[Dict], conn) -> int:
    """Upsert records into raw_player_season_stats."""
    if not records:
        return 0

    query = """
        INSERT INTO raw_player_season_stats (
            player_id, player_name, season, team_id,
            games_played, points_per_game, rebounds_per_game, assists_per_game,
            steals_per_game, blocks_per_game, fg_pct, three_pt_pct, ft_pct,
            minutes_per_game, player_efficiency_rating, win_shares, raw_data
        ) VALUES %s
        ON CONFLICT (player_id, season) DO UPDATE SET
            player_name             = EXCLUDED.player_name,
            team_id                 = EXCLUDED.team_id,
            games_played            = EXCLUDED.games_played,
            points_per_game         = EXCLUDED.points_per_game,
            rebounds_per_game       = EXCLUDED.rebounds_per_game,
            assists_per_game        = EXCLUDED.assists_per_game,
            steals_per_game         = EXCLUDED.steals_per_game,
            blocks_per_game         = EXCLUDED.blocks_per_game,
            fg_pct                  = EXCLUDED.fg_pct,
            three_pt_pct            = EXCLUDED.three_pt_pct,
            ft_pct                  = EXCLUDED.ft_pct,
            minutes_per_game        = EXCLUDED.minutes_per_game,
            player_efficiency_rating = EXCLUDED.player_efficiency_rating,
            win_shares              = EXCLUDED.win_shares,
            raw_data                = EXCLUDED.raw_data,
            ingested_at             = CURRENT_TIMESTAMP
    """

    values = [
        (
            r['player_id'], r['player_name'], r['season'], r['team_id'],
            r['games_played'], r['points_per_game'], r['rebounds_per_game'], r['assists_per_game'],
            r['steals_per_game'], r['blocks_per_game'], r['fg_pct'], r['three_pt_pct'], r['ft_pct'],
            r['minutes_per_game'], r['player_efficiency_rating'], r['win_shares'],
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
    Main entry point. For each MVP season:
    1. Fetch all player per-game stats
    2. Store league-wide averages (rotation players) in raw_league_season_averages
    3. Fetch team wins (for MVP profile team_wins field)
    4. Store the MVP player's stats in raw_player_season_stats
    """
    seasons = seasons or MVP_SEASONS
    conn = get_db_conn()
    mvp_list = get_mvp_players(conn)
    mvp_by_season = {m['season']: m for m in mvp_list}

    total_inserted = 0
    results = {}
    skipped = []

    try:
        for season in seasons:
            mvp = mvp_by_season.get(season)
            if not mvp:
                logger.warning(f"No MVP found for {season}, skipping")
                skipped.append(season)
                continue

            # Fetch the full-league stats table once, use it for both the
            # MVP row and the league-average context stats
            league_df = fetch_league_player_df(season)
            if league_df is None or league_df.empty:
                logger.warning(f"Empty player stats response for {season}")
                skipped.append(season)
                results[season] = {'rows': 0, 'status': 'api_failed'}
                continue

            player_stats = build_player_stats_dict(league_df)

            league_avg = compute_league_averages(league_df, season)
            if league_avg:
                insert_league_averages([league_avg], conn)

            # Fetch team wins
            team_wins = fetch_team_wins_for_season(season)

            # Build record for this MVP
            mvp_id = mvp['player_id']
            stats = player_stats.get(mvp_id)
            if not stats:
                logger.warning(f"MVP {mvp['player_name']} not found in {season} player stats")
                skipped.append(season)
                results[season] = {'rows': 0, 'status': 'player_not_found'}
                continue

            # Attach team wins to raw_data for later use in analytics ETL
            team_id = stats.get('team_id')
            wins = team_wins.get(team_id) if team_id else None
            stats['raw_data']['team_wins'] = wins

            record = {
                'player_id': mvp_id,
                'season': season,
                **stats
            }

            inserted = insert_player_season_stats([record], conn)
            total_inserted += inserted
            results[season] = {'rows': inserted, 'status': 'ok', 'team_wins': wins}

            logger.info(
                f"  -> Stored {mvp['player_name']} ({season}): "
                f"{stats['points_per_game']} PPG, {stats['rebounds_per_game']} RPG, "
                f"{stats['assists_per_game']} APG | Team wins: {wins}"
            )

    finally:
        conn.close()

    logger.info(f"Done. Total rows inserted/updated: {total_inserted}")
    if skipped:
        logger.warning(f"Skipped seasons: {skipped}")

    return {'total': total_inserted, 'by_season': results, 'skipped': skipped}


if __name__ == '__main__':
    summary = run()
    print("\n===== MVP PLAYER STATS INGESTION SUMMARY =====")
    for season, info in summary['by_season'].items():
        if info['rows'] > 0:
            print(f"  {season}: stored ({info.get('team_wins', '?')} team wins)")
        else:
            print(f"  {season}: SKIPPED — {info['status']}")
    print(f"\n  TOTAL ROWS: {summary['total']}")
    print("===============================================")
