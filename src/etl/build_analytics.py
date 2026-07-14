"""
Runs all three analytics builders in sequence. Each one truncates and
rebuilds its own table from raw/processed data, so this is safe to rerun.
"""

import logging
import sys

from .build_mvp_profiles import build as build_mvp_profiles
from .build_playoff_upsets import build as build_playoff_upsets
from .build_shooting_zones import build as build_shooting_zones

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run() -> dict:
    logger.info("Building player_shooting_zones ...")
    zones = build_shooting_zones()

    logger.info("Building playoff_upsets ...")
    upsets = build_playoff_upsets()

    logger.info("Building mvp_season_profiles ...")
    profiles = build_mvp_profiles()

    return {
        'player_shooting_zones': zones['rows_built'],
        'playoff_upsets': upsets['rows_built'],
        'mvp_season_profiles': profiles['rows_built'],
    }


if __name__ == '__main__':
    result = run()
    print("\n===== ANALYTICS BUILD SUMMARY =====")
    for table, rows in result.items():
        print(f"  {table}: {rows} rows")
    print("====================================")
