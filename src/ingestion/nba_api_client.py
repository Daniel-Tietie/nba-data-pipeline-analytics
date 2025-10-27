"""
NBA API client for fetching game schedules and team statistics.
Uses the nba_api library to interact with NBA stats endpoints.
"""

from nba_api.stats.endpoints import leaguegamefinder, teamgamelog, leaguedashteamstats
from nba_api.stats.static import teams as nba_teams
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import json
import math
from typing import List, Dict, Any, Optional
from .config import (
    CURRENT_SEASON, API_DELAY_SECONDS, MAX_RETRIES, 
    RETRY_DELAY, DAYS_TO_FETCH_BACK, DAYS_TO_FETCH_AHEAD
)

logger = logging.getLogger(__name__)


def clean_json_data(data: dict) -> dict:
    """
    Clean dictionary data to be JSON-serializable.
    Replaces NaN, Infinity with None.
    
    Args:
        data: Dictionary potentially containing NaN values
    
    Returns:
        Cleaned dictionary safe for JSON serialization
    """
    cleaned = {}
    for key, value in data.items():
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        elif isinstance(value, dict):
            cleaned[key] = clean_json_data(value)
        elif isinstance(value, list):
            cleaned[key] = [clean_json_data(item) if isinstance(item, dict) else item for item in value]
        else:
            cleaned[key] = value
    return cleaned


class NBAApiClient:
    """Client for fetching NBA data from official stats API."""
    
    def __init__(self):
        """Initialize NBA API client."""
        self.current_season = CURRENT_SEASON
        logger.info(f"Initialized NBA API client for season {self.current_season}")
    
    def _api_call_with_retry(self, api_func, **kwargs) -> Optional[pd.DataFrame]:
        """
        Make API call with retry logic.
        
        Args:
            api_func: API function to call
            **kwargs: Arguments to pass to API function
        
        Returns:
            DataFrame from API or None if all retries fail
        """
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(API_DELAY_SECONDS)  # Rate limiting
                result = api_func(**kwargs)
                df = result.get_data_frames()[0]
                logger.debug(f"API call successful: {api_func.__name__}")
                return df
            except Exception as e:
                logger.warning(f"API call attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"All API call attempts failed for {api_func.__name__}")
                    return None
        return None
    
    def fetch_games_by_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch all NBA games within a date range.
        
        Args:
            start_date: Start date for games
            end_date: End date for games
        
        Returns:
            List of game dictionaries
        """
        logger.info(f"Fetching games from {start_date.date()} to {end_date.date()}")
        
        df = self._api_call_with_retry(
            leaguegamefinder.LeagueGameFinder,
            season_nullable=self.current_season,
            league_id_nullable='00',  # NBA
            date_from_nullable=start_date.strftime('%m/%d/%Y'),
            date_to_nullable=end_date.strftime('%m/%d/%Y')
        )
        
        if df is None or df.empty:
            logger.warning("No games found in date range")
            return []
        
        # Process games - LeagueGameFinder returns one row per team per game
        # We need to combine home/away into single game records
        games = self._process_games_dataframe(df)
        
        logger.info(f"Processed {len(games)} games from API")
        return games
    
    def _process_games_dataframe(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Process raw game dataframe into structured game records.
        Each game appears twice in the raw data (once per team).
        
        Args:
            df: Raw games dataframe from API
        
        Returns:
            List of unique game dictionaries
        """
        games_dict = {}
        
        for _, row in df.iterrows():
            game_id = str(row['GAME_ID'])
            game_date = pd.to_datetime(row['GAME_DATE']).date()
            team_id = int(row['TEAM_ID'])
            matchup = str(row['MATCHUP'])
            
            # Determine if team was home or away
            is_home = '@' not in matchup
            
            if game_id not in games_dict:
                games_dict[game_id] = {
                    'game_id': game_id,
                    'game_date': game_date,
                    'season': self.current_season,
                    'game_status': 'final' if row.get('PTS') else 'scheduled',
                    'raw_data': {}
                }
            
            # Add team data to appropriate side
            if is_home:
                games_dict[game_id]['home_team_id'] = team_id
                games_dict[game_id]['home_team_score'] = int(row['PTS']) if pd.notna(row.get('PTS')) else None
            else:
                games_dict[game_id]['away_team_id'] = team_id
                games_dict[game_id]['away_team_score'] = int(row['PTS']) if pd.notna(row.get('PTS')) else None
            
            # Store full row data (clean NaN values for JSON compatibility)
            games_dict[game_id]['raw_data'][f"team_{team_id}"] = clean_json_data(row.to_dict())
        
        # Filter out incomplete games (missing home or away team)
        complete_games = [
            game for game in games_dict.values()
            if 'home_team_id' in game and 'away_team_id' in game
        ]
        
        logger.info(f"Filtered to {len(complete_games)} complete games")
        return complete_games
    
    def fetch_team_stats(self, team_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Fetch current season statistics for specified teams.
        
        Args:
            team_ids: List of NBA team IDs
        
        Returns:
            List of team stat dictionaries
        """
        logger.info(f"Fetching stats for {len(team_ids)} teams")
        
        df = self._api_call_with_retry(
            leaguedashteamstats.LeagueDashTeamStats,
            season=self.current_season,
            season_type_all_star='Regular Season',
            per_mode_detailed='PerGame'
        )
        
        if df is None or df.empty:
            logger.warning("No team stats retrieved")
            return []
        
        # Filter to requested teams
        df = df[df['TEAM_ID'].isin(team_ids)]
        
        stats = []
        for _, row in df.iterrows():
            stat = {
                'team_id': int(row['TEAM_ID']),
                'game_id': None,  # Season stats, not game-specific
                'stat_date': datetime.now().date(),
                'wins': int(row['W']) if pd.notna(row.get('W')) else None,
                'losses': int(row['L']) if pd.notna(row.get('L')) else None,
                'win_pct': float(row['W_PCT']) if pd.notna(row.get('W_PCT')) else None,
                'points_per_game': float(row['PTS']) if pd.notna(row.get('PTS')) else None,
                'opp_points_per_game': float(row['OPP_PTS']) if pd.notna(row.get('OPP_PTS')) else None,
                'field_goal_pct': float(row['FG_PCT']) if pd.notna(row.get('FG_PCT')) else None,
                'three_point_pct': float(row['FG3_PCT']) if pd.notna(row.get('FG3_PCT')) else None,
                'free_throw_pct': float(row['FT_PCT']) if pd.notna(row.get('FT_PCT')) else None,
                'rebounds_per_game': float(row['REB']) if pd.notna(row.get('REB')) else None,
                'assists_per_game': float(row['AST']) if pd.notna(row.get('AST')) else None,
                'raw_data': clean_json_data(row.to_dict())
            }
            stats.append(stat)
        
        logger.info(f"Processed stats for {len(stats)} teams")
        return stats
    
    def fetch_recent_games(self, days_back: int = DAYS_TO_FETCH_BACK) -> List[Dict[str, Any]]:
        """
        Fetch games from recent days.
        
        Args:
            days_back: Number of days back to fetch
        
        Returns:
            List of game dictionaries
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        return self.fetch_games_by_date_range(start_date, end_date)
    
    def fetch_upcoming_games(self, days_ahead: int = DAYS_TO_FETCH_AHEAD) -> List[Dict[str, Any]]:
        """
        Fetch scheduled upcoming games.
        
        Args:
            days_ahead: Number of days ahead to fetch
        
        Returns:
            List of game dictionaries
        """
        start_date = datetime.now()
        end_date = start_date + timedelta(days=days_ahead)
        return self.fetch_games_by_date_range(start_date, end_date)