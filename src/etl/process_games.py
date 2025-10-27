"""
Process raw game data into clean, analysis-ready format.
Determines winners, calculates point differentials, validates data.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import sys
sys.path.append('..')
from src.ingestion.database import DatabaseConnection

logger = logging.getLogger(__name__)


def process_raw_games() -> Dict[str, Any]:
    """
    Process raw games into clean games table.
    
    Steps:
    1. Read from raw_games
    2. Determine winner
    3. Calculate point differential
    4. Insert into games table
    
    Returns:
        Dictionary with processing results
    """
    logger.info("Starting raw games processing")
    start_time = datetime.now()
    
    with DatabaseConnection() as db:
        # SQL to process raw games into clean games
        process_query = """
            INSERT INTO games (
                game_id, game_date, season, home_team_id, away_team_id,
                home_score, away_score, winner_id, point_differential, 
                total_score, game_status
            )
            SELECT 
                rg.game_id,
                rg.game_date,
                rg.season,
                rg.home_team_id,
                rg.away_team_id,
                rg.home_team_score,
                rg.away_team_score,
                CASE 
                    WHEN rg.home_team_score > rg.away_team_score THEN rg.home_team_id
                    WHEN rg.away_team_score > rg.home_team_score THEN rg.away_team_id
                    ELSE NULL
                END as winner_id,
                ABS(rg.home_team_score - rg.away_team_score) as point_differential,
                rg.home_team_score + rg.away_team_score as total_score,
                rg.game_status
            FROM raw_games rg
            WHERE rg.home_team_score IS NOT NULL 
                AND rg.away_team_score IS NOT NULL
                AND rg.game_status = 'final'
            ON CONFLICT (game_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                winner_id = EXCLUDED.winner_id,
                point_differential = EXCLUDED.point_differential,
                total_score = EXCLUDED.total_score,
                processed_at = CURRENT_TIMESTAMP
        """
        
        try:
            db.cursor.execute(process_query)
            games_processed = db.cursor.rowcount
            logger.info(f"Processed {games_processed} games")
            
            # Get some stats
            db.cursor.execute("""
                SELECT 
                    season,
                    COUNT(*) as game_count,
                    AVG(total_score) as avg_total_score,
                    AVG(point_differential) as avg_margin
                FROM games
                GROUP BY season
                ORDER BY season
            """)
            
            season_stats = db.cursor.fetchall()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'success': True,
                'games_processed': games_processed,
                'duration_seconds': duration,
                'season_breakdown': [
                    {
                        'season': row[0],
                        'games': row[1],
                        'avg_total_score': float(row[2]) if row[2] else 0,
                        'avg_margin': float(row[3]) if row[3] else 0
                    }
                    for row in season_stats
                ]
            }
            
            logger.info(f"Games processing complete in {duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process games: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'games_processed': 0
            }


def get_games_summary() -> Dict[str, Any]:
    """
    Get summary statistics of processed games.
    
    Returns:
        Dictionary with game statistics
    """
    with DatabaseConnection() as db:
        db.cursor.execute("""
            SELECT 
                COUNT(*) as total_games,
                COUNT(DISTINCT season) as seasons,
                MIN(game_date) as first_game,
                MAX(game_date) as last_game,
                AVG(total_score) as avg_total_score,
                AVG(point_differential) as avg_margin
            FROM games
        """)
        
        row = db.cursor.fetchone()
        
        return {
            'total_games': row[0],
            'seasons': row[1],
            'first_game': row[2],
            'last_game': row[3],
            'avg_total_score': float(row[4]) if row[4] else 0,
            'avg_margin': float(row[5]) if row[5] else 0
        }


if __name__ == "__main__":
    """Run game processing standalone."""
    import os
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("PROCESSING RAW GAMES")
    print("=" * 60)
    
    result = process_raw_games()
    
    if result['success']:
        print(f"\nSUCCESS: Processed {result['games_processed']} games")
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print("\nBy Season:")
        for season in result['season_breakdown']:
            print(f"  {season['season']}: {season['games']} games, "
                  f"Avg Score: {season['avg_total_score']:.1f}, "
                  f"Avg Margin: {season['avg_margin']:.1f}")
        
        print("\nOverall Summary:")
        summary = get_games_summary()
        print(f"  Total Games: {summary['total_games']}")
        print(f"  Date Range: {summary['first_game']} to {summary['last_game']}")
        print(f"  Avg Total Score: {summary['avg_total_score']:.1f}")
        print(f"  Avg Margin: {summary['avg_margin']:.1f}")
    else:
        print(f"\nFAILED: {result.get('error', 'Unknown error')}")
        sys.exit(1)
    
    print("=" * 60)