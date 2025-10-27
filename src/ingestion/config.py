"""
Configuration constants for NBA data ingestion.
"""

from datetime import datetime

# Current NBA season (update yearly)
CURRENT_SEASON = "2024-25"

# Season start/end dates (approximate)
SEASON_START_DATE = datetime(2024, 10, 22)  # NBA season typically starts late October
SEASON_END_DATE = datetime(2025, 4, 13)     # Regular season ends mid-April

# API rate limiting
API_DELAY_SECONDS = 0.6  # Wait between API calls to avoid rate limits

# Data fetching parameters
DAYS_TO_FETCH_BACK = 7   # How many days back to fetch when updating
DAYS_TO_FETCH_AHEAD = 30  # How many days ahead to fetch schedules

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'