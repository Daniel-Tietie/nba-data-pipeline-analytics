"""
Main ingestion script that orchestrates data collection.
Can be run standalone or called by Airflow DAG.
"""

import logging
import sys
from datetime import datetime, timedelta
from .nba_api_client import NBAApiClient
from .database import DatabaseConnection
from .config import LOG_FORMAT, LOG_LEVEL

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/ingestion.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


def ingest_recent_games(days_back: int = 7) -> dict:
    """
    Ingest games from recent days.
    
    Args:
        days_back: Number of days back to fetch
    
    Returns:
        Dictionary with ingestion results
    """
    logger.info(f"Starting ingestion of games from last {days_back} days")
    start_time = datetime.now()
    
    try:
        # Initialize API client
        api_client = NBAApiClient()
        
        # Fetch games
        games = api_client.fetch_recent_games(days_back=days_back)
        
        if not games:
            logger.warning("No games fetched from API")
            return {
                'success': True,
                'games_fetched': 0,
                'games_inserted': 0,
                'duration_seconds': 0
            }
        
        # Insert into database
        with DatabaseConnection() as db:
            games_inserted = db.insert_raw_games(games)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Ingestion complete: {games_inserted} games in {duration:.2f}s")
        
        return {
            'success': True,
            'games_fetched': len(games),
            'games_inserted': games_inserted,
            'duration_seconds': duration
        }
    
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'games_fetched': 0,
            'games_inserted': 0
        }


def ingest_team_stats() -> dict:
    """
    Ingest current team statistics for all teams.
    
    Returns:
        Dictionary with ingestion results
    """
    logger.info("Starting ingestion of team statistics")
    start_time = datetime.now()
    
    try:
        # Initialize API client
        api_client = NBAApiClient()
        
        # Get team IDs from database
        with DatabaseConnection() as db:
            team_ids = db.get_team_ids()
        
        if not team_ids:
            logger.error("No teams found in database")
            return {'success': False, 'error': 'No teams in database'}
        
        # Fetch team stats
        stats = api_client.fetch_team_stats(team_ids)
        
        if not stats:
            logger.warning("No team stats fetched from API")
            return {
                'success': True,
                'stats_fetched': 0,
                'stats_inserted': 0,
                'duration_seconds': 0
            }
        
        # Insert into database
        with DatabaseConnection() as db:
            stats_inserted = db.insert_raw_team_stats(stats)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Team stats ingestion complete: {stats_inserted} records in {duration:.2f}s")
        
        return {
            'success': True,
            'stats_fetched': len(stats),
            'stats_inserted': stats_inserted,
            'duration_seconds': duration
        }
    
    except Exception as e:
        logger.error(f"Team stats ingestion failed: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'stats_fetched': 0,
            'stats_inserted': 0
        }


def ingest_historical_games(
    season: str = "2023-24", 
    start_date: str = None,
    end_date: str = None,
    days: int = None
) -> dict:
    """
    Ingest historical games from a completed season.
    
    Args:
        season: NBA season in format "YYYY-YY" (e.g., "2023-24")
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        days: Number of days if not using explicit dates (optional)
    
    Returns:
        Dictionary with ingestion results
    """
    logger.info(f"Starting ingestion of historical games from {season} season")
    start_time = datetime.now()
    
    try:
        # Initialize API client with historical season
        api_client = NBAApiClient()
        api_client.current_season = season
        
        # Determine date range
        if start_date and end_date:
            # Use explicit dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        elif days:
            # Use days parameter with season end date
            season_end_dates = {
                "2023-24": datetime(2024, 4, 14),
                "2022-23": datetime(2023, 4, 9),
                "2021-22": datetime(2022, 4, 10),
                "2020-21": datetime(2021, 5, 16)
            }
            end_dt = season_end_dates.get(season, datetime.now())
            start_dt = end_dt - timedelta(days=days)
        else:
            # Default: entire season
            season_dates = {
                "2023-24": (datetime(2023, 10, 24), datetime(2024, 4, 14)),
                "2022-23": (datetime(2022, 10, 18), datetime(2023, 4, 9)),
                "2021-22": (datetime(2021, 10, 19), datetime(2022, 4, 10)),
                "2020-21": (datetime(2020, 12, 22), datetime(2021, 5, 16))
            }
            start_dt, end_dt = season_dates.get(season, (datetime.now() - timedelta(days=30), datetime.now()))
        
        logger.info(f"Fetching games from {start_dt.date()} to {end_dt.date()}")
        
        games = api_client.fetch_games_by_date_range(start_dt, end_dt)
        
        if not games:
            logger.warning(f"No games fetched for season {season}")
            return {
                'success': True,
                'games_fetched': 0,
                'games_inserted': 0,
                'duration_seconds': 0,
                'season': season
            }
        
        # Insert into database
        with DatabaseConnection() as db:
            games_inserted = db.insert_raw_games(games)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Historical ingestion complete: {games_inserted} games in {duration:.2f}s")
        
        return {
            'success': True,
            'games_fetched': len(games),
            'games_inserted': games_inserted,
            'duration_seconds': duration,
            'season': season,
            'date_range': f"{start_dt.date()} to {end_dt.date()}"
        }
    
    except Exception as e:
        logger.error(f"Historical ingestion failed: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'games_fetched': 0,
            'games_inserted': 0,
            'season': season
        }


def ingest_multiple_seasons(seasons: list) -> dict:
    """
    Ingest complete data for multiple NBA seasons.
    
    Args:
        seasons: List of season strings (e.g., ["2021-22", "2022-23", "2023-24"])
    
    Returns:
        Dictionary with combined results
    """
    logger.info("=" * 50)
    logger.info(f"Starting multi-season ingestion for {len(seasons)} seasons")
    logger.info("=" * 50)
    
    all_results = []
    total_games = 0
    overall_success = True
    
    for season in seasons:
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Processing season: {season}")
        logger.info('=' * 50)
        
        result = ingest_historical_games(season=season)
        all_results.append(result)
        
        if result['success']:
            total_games += result['games_inserted']
            logger.info(f"Season {season}: {result['games_inserted']} games ingested")
        else:
            overall_success = False
            logger.error(f"Season {season}: Failed - {result.get('error', 'Unknown error')}")
    
    logger.info("\n" + "=" * 50)
    logger.info(f"Multi-season ingestion complete. Total games: {total_games}")
    logger.info("=" * 50)
    
    return {
        'success': overall_success,
        'total_games': total_games,
        'seasons': all_results,
        'timestamp': datetime.now().isoformat()
    }


def run_full_ingestion() -> dict:
    """
    Run complete ingestion: games and team stats.
    
    Returns:
        Dictionary with combined results
    """
    logger.info("=" * 50)
    logger.info("Starting full ingestion pipeline")
    logger.info("=" * 50)
    
    # Ingest games
    games_result = ingest_recent_games()
    
    # Ingest team stats
    stats_result = ingest_team_stats()
    
    # Combine results
    overall_success = games_result['success'] and stats_result['success']
    
    result = {
        'success': overall_success,
        'games': games_result,
        'team_stats': stats_result,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info("=" * 50)
    logger.info(f"Full ingestion complete. Success: {overall_success}")
    logger.info("=" * 50)
    
    return result


if __name__ == "__main__":
    """Run ingestion when script is executed directly."""
    import os
    import argparse
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NBA Data Ingestion')
    parser.add_argument('--historical', action='store_true', 
                       help='Fetch historical games')
    parser.add_argument('--multi-season', action='store_true',
                       help='Fetch multiple complete seasons')
    parser.add_argument('--season', type=str, default='2023-24',
                       help='Season to fetch (e.g., 2023-24)')
    parser.add_argument('--seasons', type=str, nargs='+',
                       default=['2021-22', '2022-23', '2023-24'],
                       help='Multiple seasons for multi-season mode')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days worth of games to fetch')
    parser.add_argument('--start-date', type=str,
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                       help='End date in YYYY-MM-DD format')
    args = parser.parse_args()
    
    if args.multi_season:
        # Fetch multiple complete seasons
        logger.info(f"Running multi-season ingestion for: {', '.join(args.seasons)}")
        result = ingest_multiple_seasons(args.seasons)
        
        print("\n" + "=" * 60)
        print("MULTI-SEASON INGESTION SUMMARY")
        print("=" * 60)
        print(f"Overall Success: {result['success']}")
        print(f"Total Games Ingested: {result['total_games']}")
        print("\nBy Season:")
        for season_result in result['seasons']:
            status = "SUCCESS" if season_result['success'] else "FAILED"
            print(f"  {status} {season_result.get('season', 'Unknown')}: "
                  f"{season_result['games_inserted']} games "
                  f"({season_result.get('date_range', 'N/A')})")
        print("=" * 60)
        
        sys.exit(0 if result['success'] else 1)
    
    elif args.historical:
        # Fetch historical games for single season
        logger.info(f"Running historical ingestion for season {args.season}")
        result = ingest_historical_games(
            season=args.season,
            start_date=args.start_date,
            end_date=args.end_date,
            days=args.days if not (args.start_date and args.end_date) else None
        )
        
        print("\n" + "=" * 50)
        print("HISTORICAL INGESTION SUMMARY")
        print("=" * 50)
        print(f"Season: {result.get('season', args.season)}")
        print(f"Date Range: {result.get('date_range', 'N/A')}")
        print(f"Success: {result['success']}")
        print(f"Games Fetched: {result['games_fetched']}")
        print(f"Games Inserted: {result['games_inserted']}")
        print(f"Duration: {result.get('duration_seconds', 0):.2f}s")
        print("=" * 50)
        
        sys.exit(0 if result['success'] else 1)
    
    else:
        # Run current season ingestion
        result = run_full_ingestion()
        
        # Print summary
        print("\n" + "=" * 50)
        print("INGESTION SUMMARY")
        print("=" * 50)
        print(f"Overall Success: {result['success']}")
        print(f"\nGames:")
        print(f"  - Fetched: {result['games']['games_fetched']}")
        print(f"  - Inserted: {result['games']['games_inserted']}")
        print(f"\nTeam Stats:")
        print(f"  - Fetched: {result['team_stats']['stats_fetched']}")
        print(f"  - Inserted: {result['team_stats']['stats_inserted']}")
        print("=" * 50)
        
        sys.exit(0 if result['success'] else 1)