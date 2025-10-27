"""
Configuration for ETL pipeline.
"""

# Feature engineering parameters
ROLLING_WINDOW_GAMES = 10  # Calculate stats over last N games
MIN_GAMES_FOR_STATS = 5    # Minimum games needed for rolling stats
HEAD_TO_HEAD_LOOKBACK = 10  # How many past H2H games to consider

# Team performance thresholds
HOME_ADVANTAGE_FACTOR = 1.05  # Typical home court advantage
BACK_TO_BACK_PENALTY = 0.95   # Performance drop on back-to-back games

# Data quality thresholds
MAX_MISSING_PCT = 0.05  # Maximum 5% missing data allowed
MIN_EXPECTED_GAMES = 1000  # Minimum games expected per season

# Logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'