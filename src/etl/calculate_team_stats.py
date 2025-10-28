"""
Calculate aggregated team statistics over time.
Computes rolling averages, win streaks, and performance metrics.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import sys
sys.path.append('..')
from src.ingestion.database import DatabaseConnection
from .config import ROLLING_WINDOW_GAMES

logger = logging.getLogger(__name__)


def calculate_team_stats() -> Dict[str, Any]:
    """
    Calculate team statistics for all teams across all games.
    
    For each team on each date after a game, calculate:
    - Season record (wins, losses, win_pct)
    - Rolling averages (points, opp_points, rebounds, assists, etc.)
    - Recent form (last 5 games)
    - Home/away splits
    
    Returns:
        Dictionary with calculation results
    """
    logger.info("Starting team statistics calculation")
    start_time = datetime.now()
    
    with DatabaseConnection() as db:
        # First, clear existing team_stats
        db.cursor.execute("DELETE FROM team_stats")
        logger.info("Cleared existing team stats")
        
        # Calculate comprehensive team stats
        # This is a complex query that aggregates game data for each team
        stats_query = """
            WITH game_results AS (
                -- Get each team's games with their performance
                SELECT 
                    g.season,
                    g.game_date,
                    g.home_team_id as team_id,
                    g.home_score as points,
                    g.away_score as opp_points,
                    CASE WHEN g.winner_id = g.home_team_id THEN 1 ELSE 0 END as won,
                    1 as is_home
                FROM games g
                WHERE g.game_status = 'final'
                
                UNION ALL
                
                SELECT 
                    g.season,
                    g.game_date,
                    g.away_team_id as team_id,
                    g.away_score as points,
                    g.home_score as opp_points,
                    CASE WHEN g.winner_id = g.away_team_id THEN 1 ELSE 0 END as won,
                    0 as is_home
                FROM games g
                WHERE g.game_status = 'final'
            ),
            team_game_stats AS (
                -- Calculate running stats for each team
                SELECT 
                    season,
                    team_id,
                    game_date as stat_date,
                    COUNT(*) OVER (PARTITION BY team_id, season ORDER BY game_date) as games_played,
                    SUM(won) OVER (PARTITION BY team_id, season ORDER BY game_date) as wins,
                    COUNT(*) OVER (PARTITION BY team_id, season ORDER BY game_date) - 
                        SUM(won) OVER (PARTITION BY team_id, season ORDER BY game_date) as losses,
                    AVG(points) OVER (
                        PARTITION BY team_id, season 
                        ORDER BY game_date 
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                    ) as avg_points,
                    AVG(opp_points) OVER (
                        PARTITION BY team_id, season 
                        ORDER BY game_date 
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                    ) as avg_opp_points,
                    SUM(CASE WHEN is_home = 1 THEN won ELSE 0 END) OVER (
                        PARTITION BY team_id, season ORDER BY game_date
                    ) as home_wins,
                    SUM(CASE WHEN is_home = 1 THEN 1 ELSE 0 END) OVER (
                        PARTITION BY team_id, season ORDER BY game_date
                    ) as home_games,
                    SUM(CASE WHEN is_home = 0 THEN won ELSE 0 END) OVER (
                        PARTITION BY team_id, season ORDER BY game_date
                    ) as away_wins,
                    SUM(CASE WHEN is_home = 0 THEN 1 ELSE 0 END) OVER (
                        PARTITION BY team_id, season ORDER BY game_date
                    ) as away_games,
                    -- Last 5 games
                    SUM(won) OVER (
                        PARTITION BY team_id, season 
                        ORDER BY game_date 
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) as last_5_wins
                FROM game_results
            )
            INSERT INTO team_stats (
                team_id, stat_date, season, games_played, wins, losses,
                win_pct, avg_points, avg_opp_points, point_diff,
                home_record, away_record, last_5_record
            )
            SELECT DISTINCT ON (team_id, stat_date) 
                team_id,
                stat_date,
                season,
                games_played,
                wins,
                losses,
                CASE WHEN games_played > 0 THEN ROUND(wins::NUMERIC / games_played::NUMERIC, 3) ELSE 0 END as win_pct,
                ROUND(avg_points::NUMERIC, 1) as avg_points,
                ROUND(avg_opp_points::NUMERIC, 1) as avg_opp_points,
                ROUND((avg_points - avg_opp_points)::NUMERIC, 1) as point_diff,
                home_wins || '-' || (home_games - home_wins) as home_record,
                away_wins || '-' || (away_games - away_wins) as away_record,
                last_5_wins || '-' || LEAST(5, games_played) - last_5_wins as last_5_record
            FROM team_game_stats
            WHERE games_played > 0
            ORDER BY team_id, stat_date, games_played DESC
            ON CONFLICT (team_id, stat_date) DO UPDATE SET
                games_played = EXCLUDED.games_played,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                win_pct = EXCLUDED.win_pct,
                avg_points = EXCLUDED.avg_points,
                avg_opp_points = EXCLUDED.avg_opp_points,
                point_diff = EXCLUDED.point_diff,
                home_record = EXCLUDED.home_record,
                away_record = EXCLUDED.away_record,
                last_5_record = EXCLUDED.last_5_record
        """
        
        try:
            db.cursor.execute(stats_query)
            stats_inserted = db.cursor.rowcount
            logger.info(f"Calculated stats for {stats_inserted} team-date combinations")
            
            # Get summary by season
            db.cursor.execute("""
                SELECT 
                    season,
                    COUNT(DISTINCT team_id) as teams,
                    COUNT(*) as total_records,
                    AVG(games_played) as avg_games_per_team,
                    AVG(avg_points) as league_avg_points
                FROM team_stats
                GROUP BY season
                ORDER BY season
            """)
            
            season_summary = db.cursor.fetchall()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'success': True,
                'stats_calculated': stats_inserted,
                'duration_seconds': duration,
                'season_breakdown': [
                    {
                        'season': row[0],
                        'teams': row[1],
                        'records': row[2],
                        'avg_games': float(row[3]) if row[3] else 0,
                        'league_avg_points': float(row[4]) if row[4] else 0
                    }
                    for row in season_summary
                ]
            }
            
            logger.info(f"Team stats calculation complete in {duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Failed to calculate team stats: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'stats_calculated': 0
            }


def get_team_stats_sample(team_id: int = None, limit: int = 10) -> None:
    """
    Print sample team statistics for verification.
    
    Args:
        team_id: Specific team to show (None for random team)
        limit: Number of records to show
    """
    with DatabaseConnection() as db:
        if team_id:
            query = """
                SELECT ts.stat_date, t.team_name, ts.games_played, 
                       ts.wins, ts.losses, ts.win_pct,
                       ts.avg_points, ts.avg_opp_points, ts.point_diff,
                       ts.last_5_record
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.team_id
                WHERE ts.team_id = %s
                ORDER BY ts.stat_date DESC
                LIMIT %s
            """
            db.cursor.execute(query, (team_id, limit))
        else:
            query = """
                SELECT ts.stat_date, t.team_name, ts.games_played,
                       ts.wins, ts.losses, ts.win_pct,
                       ts.avg_points, ts.avg_opp_points, ts.point_diff,
                       ts.last_5_record
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.team_id
                ORDER BY ts.stat_date DESC
                LIMIT %s
            """
            db.cursor.execute(query, (limit,))
        
        rows = db.cursor.fetchall()
        
        print("\nSample Team Statistics:")
        print("-" * 120)
        print(f"{'Date':<12} {'Team':<25} {'GP':>4} {'Record':>8} {'Win%':>6} {'PPG':>6} {'OppPPG':>7} {'Diff':>6} {'Last5':>7}")
        print("-" * 120)
        
        for row in rows:
            date, team, gp, w, l, wpct, ppg, opp, diff, last5 = row
            record = f"{w}-{l}"
            print(f"{date} {team:<25} {gp:>4} {record:>8} {wpct:>6.3f} {ppg:>6.1f} {opp:>7.1f} {diff:>6.1f} {last5:>7}")


if __name__ == "__main__":
    """Run team stats calculation standalone."""
    import os
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("CALCULATING TEAM STATISTICS")
    print("=" * 60)
    
    result = calculate_team_stats()
    
    if result['success']:
        print(f"\nSUCCESS: Calculated {result['stats_calculated']} team stat records")
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print("\nBy Season:")
        for season in result['season_breakdown']:
            print(f"  {season['season']}: {season['teams']} teams, "
                  f"{season['records']} records, "
                  f"Avg {season['avg_games']:.1f} games/team, "
                  f"League Avg: {season['league_avg_points']:.1f} PPG")
        
        # Show sample data
        print("\n" + "=" * 60)
        get_team_stats_sample(team_id=1610612747, limit=10)  # Lakers
        print("=" * 60)
    else:
        print(f"\nFAILED: {result.get('error', 'Unknown error')}")
        sys.exit(1)
    
    print("=" * 60)