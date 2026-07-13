"""
Thin HTTP layer for stats.nba.com endpoints.

stats.nba.com drops requests made with Python's default TLS handshake
(requests/urllib3), which is why nba_api calls hang until timeout. Routing
the same endpoints through curl_cffi with browser impersonation gets a
normal response, so ingestion scripts fetch through here instead.
"""

import logging
import time
from typing import Dict, Optional

import pandas as pd
from curl_cffi import requests as cffi_requests

from .config import API_DELAY_SECONDS, MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger(__name__)

BASE_URL = "https://stats.nba.com/stats"

HEADERS = {
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json",
}

TIMEOUT = 30


def fetch_stats(endpoint: str, params: Dict, result_set: int = 0) -> Optional[pd.DataFrame]:
    """
    Fetch one stats.nba.com endpoint and return a result set as a DataFrame.

    Args:
        endpoint: Endpoint name, e.g. 'leaguedashplayerstats'
        params: Query parameters for the endpoint
        result_set: Index into resultSets (default first)

    Returns:
        DataFrame built from headers + rowSet, or None if all retries fail
    """
    url = f"{BASE_URL}/{endpoint}"

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(API_DELAY_SECONDS)
            resp = cffi_requests.get(
                url, params=params, headers=HEADERS,
                impersonate="chrome", timeout=TIMEOUT,
            )
            resp.raise_for_status()
            payload = resp.json()
            rs = payload["resultSets"][result_set]
            df = pd.DataFrame(rs["rowSet"], columns=rs["headers"])
            logger.debug(f"{endpoint}: {len(df)} rows")
            return df
        except Exception as e:
            logger.warning(f"{endpoint} attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))

    logger.error(f"All attempts failed for {endpoint}")
    return None


# Full parameter set stats.nba.com requires for the leaguedash* endpoints.
# Missing keys cause a 400 listing the missing property names.
LEAGUE_DASH_DEFAULTS = {
    "College": "", "Conference": "", "Country": "", "DateFrom": "",
    "DateTo": "", "Division": "", "DraftPick": "", "DraftYear": "",
    "GameScope": "", "GameSegment": "", "Height": "", "LastNGames": "0",
    "LeagueID": "00", "Location": "", "MeasureType": "Base", "Month": "0",
    "OpponentTeamID": "0", "Outcome": "", "PORound": "0", "PaceAdjust": "N",
    "Period": "0", "PlayerExperience": "", "PlayerPosition": "",
    "PlusMinus": "N", "Rank": "N", "SeasonSegment": "", "ShotClockRange": "",
    "StarterBench": "", "TeamID": "0", "VsConference": "", "VsDivision": "",
    "Weight": "",
}


def league_dash_params(season: str, season_type: str, per_mode: str, **overrides) -> Dict:
    """Build a complete param dict for a leaguedash* endpoint."""
    params = dict(LEAGUE_DASH_DEFAULTS)
    params.update({
        "Season": season,
        "SeasonType": season_type,
        "PerMode": per_mode,
    })
    params.update(overrides)
    return params
