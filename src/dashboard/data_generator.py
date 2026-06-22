"""
Mock/sample data for the three NBA analytics modules:
shooting zones, playoff upset tracking, and MVP profiles.

This is a demo build. No live data or API calls — numbers below are
illustrative sample data, not verified historical records. Swap these
functions for real sources (your Airflow/Postgres pipeline) when ready.

Player headshots and team logos are hot-linked from NBA.com's public
CDN using real player/team IDs, so faces and logos render correctly
even though the surrounding stats are mock data.
"""

import numpy as np
import pandas as pd

np.random.seed(7)

# ---------- real player/team reference data (IDs verified against nba.com) ----------

TEAMS = {
    "GSW": 1610612744, "DEN": 1610612743, "MIL": 1610612749,
    "OKC": 1610612760, "LAL": 1610612747, "BOS": 1610612738,
    "PHI": 1610612755, "DAL": 1610612742,
}

PLAYERS = [
    {"name": "Stephen Curry", "team": "GSW", "pos": "PG", "player_id": 201939},
    {"name": "Nikola Jokic", "team": "DEN", "pos": "C", "player_id": 203999},
    {"name": "Giannis Antetokounmpo", "team": "MIL", "pos": "PF", "player_id": 203507},
    {"name": "Shai Gilgeous-Alexander", "team": "OKC", "pos": "PG", "player_id": 1628983},
    {"name": "Luka Doncic", "team": "LAL", "pos": "PG", "player_id": 1629029},
]


def headshot_url(player_id: int) -> str:
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"


def logo_url(team_abbr: str) -> str:
    return f"https://cdn.nba.com/logos/nba/{TEAMS[team_abbr]}/global/L/logo.svg"


def get_players() -> list[dict]:
    out = []
    for p in PLAYERS:
        row = dict(p)
        row["headshot"] = headshot_url(p["player_id"])
        row["logo"] = logo_url(p["team"])
        out.append(row)
    return out


# ---------- Module 1: shooting zone analysis ----------

# Court coordinates (feet), basket center at (0, 5.25), baseline at y=0.
# Each zone has one or more marker positions used to plot bubbles on the
# half-court diagram.
ZONE_MARKERS = {
    "Restricted Area": [(0, 3)],
    "Paint (non-RA)": [(0, 13)],
    "Mid-range": [(-14, 17), (14, 17)],
    "Corner 3": [(-23.5, 3), (23.5, 3)],
    "Above-the-break 3": [(-16, 27), (0, 31), (16, 27)],
}

# Hand-set baseline tendencies per player (attempt share, FG%) so the mock
# data reflects each player's real-world shooting profile in rough shape,
# even though exact figures are illustrative, not measured.
PLAYER_ZONE_PROFILE = {
    "Stephen Curry": {
        "Restricted Area": (0.16, 0.63), "Paint (non-RA)": (0.10, 0.42),
        "Mid-range": (0.13, 0.45), "Corner 3": (0.12, 0.45),
        "Above-the-break 3": (0.49, 0.41),
    },
    "Nikola Jokic": {
        "Restricted Area": (0.34, 0.74), "Paint (non-RA)": (0.24, 0.49),
        "Mid-range": (0.22, 0.50), "Corner 3": (0.06, 0.38),
        "Above-the-break 3": (0.14, 0.36),
    },
    "Giannis Antetokounmpo": {
        "Restricted Area": (0.48, 0.76), "Paint (non-RA)": (0.20, 0.44),
        "Mid-range": (0.12, 0.38), "Corner 3": (0.05, 0.30),
        "Above-the-break 3": (0.15, 0.27),
    },
    "Shai Gilgeous-Alexander": {
        "Restricted Area": (0.30, 0.68), "Paint (non-RA)": (0.22, 0.47),
        "Mid-range": (0.25, 0.48), "Corner 3": (0.05, 0.36),
        "Above-the-break 3": (0.18, 0.38),
    },
    "Luka Doncic": {
        "Restricted Area": (0.24, 0.64), "Paint (non-RA)": (0.16, 0.43),
        "Mid-range": (0.16, 0.41), "Corner 3": (0.08, 0.37),
        "Above-the-break 3": (0.36, 0.36),
    },
}

LEAGUE_AVG_ZONE = {
    "Restricted Area": (0.30, 0.63), "Paint (non-RA)": (0.18, 0.41),
    "Mid-range": (0.16, 0.41), "Corner 3": (0.09, 0.39),
    "Above-the-break 3": (0.27, 0.36),
}


def get_shot_zones(player_name: str) -> pd.DataFrame:
    """Zone-level attempt share and FG% for one player vs league average."""
    profile = PLAYER_ZONE_PROFILE[player_name]
    rows = []
    for zone, (att_pct, fg_pct) in profile.items():
        lg_att, lg_fg = LEAGUE_AVG_ZONE[zone]
        rows.append({
            "zone": zone, "attempt_pct": round(att_pct * 100, 1),
            "fg_pct": round(fg_pct * 100, 1),
            "league_fg_pct": round(lg_fg * 100, 1),
            "markers": ZONE_MARKERS[zone],
        })
    return pd.DataFrame(rows)


# ---------- Module 2: playoff upset tracking ----------
# Sample data only — seasons and matchups below are randomly generated,
# not actual historical playoff results.

_UPSET_TEAMS = ["BOS", "PHI", "MIL", "LAL", "GSW", "DAL", "DEN", "OKC"]


def get_playoff_upsets(n_seasons: int = 10) -> pd.DataFrame:
    seasons = list(range(2015, 2015 + n_seasons))
    rows = []
    for yr in seasons:
        for conf in ["East", "West"]:
            seed1, opp = np.random.choice(_UPSET_TEAMS, size=2, replace=False)
            upset = bool(np.random.random() < 0.18)
            rows.append({
                "season": f"{yr}-{str(yr + 1)[-2:]}", "conference": conf,
                "seed1_team": seed1, "seed1_logo": logo_url(seed1),
                "opponent": opp, "opponent_logo": logo_url(opp),
                "upset": upset,
                "result": f"Lost 4-{np.random.randint(1, 4)}" if upset else f"Won 4-{np.random.randint(0, 3)}",
            })
    return pd.DataFrame(rows)


def get_upset_summary(df: pd.DataFrame) -> dict:
    total = len(df)
    upsets = int(df["upset"].sum())
    return {
        "total": total, "upsets": upsets,
        "rate": round(upsets / total * 100, 1),
    }


# ---------- Module 3: MVP profile analysis ----------
# Sample data only — stat lines below are illustrative approximations
# for demo purposes, not pulled from verified season records.

_MVP_TEAMS_EXTRA = {"HOU": 1610612745}
TEAMS.update(_MVP_TEAMS_EXTRA)

MVP_SEASONS = [
    {"season": "2025-26", "name": "Shai Gilgeous-Alexander", "team": "OKC", "player_id": 1628983,
     "pts": 31.1, "reb": 4.3, "ast": 6.6, "ts_pct": 61.5, "win_pct": 78.0},
    {"season": "2024-25", "name": "Shai Gilgeous-Alexander", "team": "OKC", "player_id": 1628983,
     "pts": 32.7, "reb": 5.0, "ast": 6.4, "ts_pct": 63.7, "win_pct": 81.7},
    {"season": "2023-24", "name": "Nikola Jokic", "team": "DEN", "player_id": 203999,
     "pts": 26.4, "reb": 12.4, "ast": 9.0, "ts_pct": 65.4, "win_pct": 69.5},
    {"season": "2022-23", "name": "Joel Embiid", "team": "PHI", "player_id": 203954,
     "pts": 33.1, "reb": 10.2, "ast": 4.2, "ts_pct": 65.5, "win_pct": 67.1},
    {"season": "2021-22", "name": "Nikola Jokic", "team": "DEN", "player_id": 203999,
     "pts": 27.1, "reb": 13.8, "ast": 7.9, "ts_pct": 67.0, "win_pct": 64.6},
    {"season": "2020-21", "name": "Nikola Jokic", "team": "DEN", "player_id": 203999,
     "pts": 26.4, "reb": 10.8, "ast": 8.3, "ts_pct": 65.0, "win_pct": 66.7},
    {"season": "2019-20", "name": "Giannis Antetokounmpo", "team": "MIL", "player_id": 203507,
     "pts": 29.5, "reb": 13.6, "ast": 5.6, "ts_pct": 63.3, "win_pct": 76.7},
    {"season": "2018-19", "name": "Giannis Antetokounmpo", "team": "MIL", "player_id": 203507,
     "pts": 27.7, "reb": 12.5, "ast": 5.9, "ts_pct": 64.4, "win_pct": 71.9},
    {"season": "2017-18", "name": "James Harden", "team": "HOU", "player_id": 201935,
     "pts": 30.4, "reb": 5.4, "ast": 8.8, "ts_pct": 61.9, "win_pct": 79.3},
    {"season": "2016-17", "name": "Russell Westbrook", "team": "OKC", "player_id": 201566,
     "pts": 31.6, "reb": 10.7, "ast": 10.4, "ts_pct": 55.4, "win_pct": 56.1},
]


def get_mvp_profiles() -> pd.DataFrame:
    df = pd.DataFrame(MVP_SEASONS)
    df["headshot"] = df["player_id"].map(headshot_url)
    df["logo"] = df["team"].map(logo_url)
    return df
