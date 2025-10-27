"""
Database connection and operations for ingestion layer.
Handles connecting to PostgreSQL and inserting raw data.
"""

import psycopg2
from psycopg2.extras import execute_values, Json
import os
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages PostgreSQL database connections and operations."""
    
    def __init__(self):
        """Initialize database connection using environment variables."""
        self.conn_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'nba_pipeline'),
            'user': os.getenv('DB_USER', 'nba_user'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.conn = None
        self.cursor = None
    
    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            self.conn.rollback()
            logger.error(f"Transaction rolled back due to error: {exc_val}")
        else:
            self.conn.commit()
        self.close()
    
    def insert_raw_games(self, games: List[Dict[str, Any]]) -> int:
        """
        Insert raw game data into database with proper batching.
    
        Args:
            games: List of game dictionaries with keys:
                - game_id, game_date, season, home_team_id, away_team_id,
                home_team_score, away_team_score, game_status, raw_data
        
        Returns:
            Number of games inserted
        """
        if not games:
            logger.warning("No games to insert")
            return 0
        
        insert_query = """
            INSERT INTO raw_games (
                game_id, game_date, season, home_team_id, away_team_id,
                home_team_score, away_team_score, game_status, raw_data
            ) VALUES %s
            ON CONFLICT (game_id) DO UPDATE SET
                home_team_score = EXCLUDED.home_team_score,
                away_team_score = EXCLUDED.away_team_score,
                game_status = EXCLUDED.game_status,
                raw_data = EXCLUDED.raw_data,
                ingested_at = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                game['game_id'],
                game['game_date'],
                game['season'],
                game['home_team_id'],
                game['away_team_id'],
                game.get('home_team_score'),
                game.get('away_team_score'),
                game.get('game_status', 'scheduled'),
                Json(game.get('raw_data', {}))
            )
            for game in games
        ]
        
        try:
            # Insert in batches to avoid issues with large inserts
            batch_size = 100
            total_inserted = 0
        
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                execute_values(self.cursor, insert_query, batch, page_size=100)
                total_inserted += len(batch)
            
                if i % 500 == 0 and i > 0:
                    logger.info(f"Progress: {i}/{len(values)} games processed")
        
            logger.info(f"Inserted/updated {total_inserted} raw games")
            return total_inserted
        except Exception as e:
            logger.error(f"Failed to insert raw games: {e}")
            raise
    
    def insert_raw_team_stats(self, stats: List[Dict[str, Any]]) -> int:
        """
        Insert raw team statistics into database.
        
        Args:
            stats: List of stat dictionaries with team performance metrics
        
        Returns:
            Number of records inserted
        """
        if not stats:
            logger.warning("No team stats to insert")
            return 0
        
        insert_query = """
            INSERT INTO raw_team_stats (
                team_id, game_id, stat_date, wins, losses, win_pct,
                points_per_game, opp_points_per_game, field_goal_pct,
                three_point_pct, free_throw_pct, rebounds_per_game,
                assists_per_game, raw_data
            ) VALUES %s
        """
        
        values = [
            (
                stat['team_id'],
                stat.get('game_id'),
                stat['stat_date'],
                stat.get('wins'),
                stat.get('losses'),
                stat.get('win_pct'),
                stat.get('points_per_game'),
                stat.get('opp_points_per_game'),
                stat.get('field_goal_pct'),
                stat.get('three_point_pct'),
                stat.get('free_throw_pct'),
                stat.get('rebounds_per_game'),
                stat.get('assists_per_game'),
                Json(stat.get('raw_data', {}))
            )
            for stat in stats
        ]
        
        try:
            execute_values(self.cursor, insert_query, values)
            count = self.cursor.rowcount
            logger.info(f"Inserted {count} team stat records")
            return count
        except Exception as e:
            logger.error(f"Failed to insert team stats: {e}")
            raise
    
    def get_team_ids(self) -> List[int]:
        """
        Get all active team IDs from database.
        
        Returns:
            List of team IDs
        """
        query = "SELECT team_id FROM teams WHERE is_active = TRUE ORDER BY team_id"
        try:
            self.cursor.execute(query)
            team_ids = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Retrieved {len(team_ids)} team IDs")
            return team_ids
        except Exception as e:
            logger.error(f"Failed to get team IDs: {e}")
            raise