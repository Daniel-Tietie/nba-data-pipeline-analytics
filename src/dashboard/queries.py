"""
Data access layer for the dashboard.

Reads Postgres when DB_HOST is set (local dev, connected to the real
pipeline). Otherwise falls back to the bundled SQLite snapshot at
data/analytics.db, since Streamlit Community Cloud can't reach a local
Postgres instance. Re-running src/etl/export_analytics.py and committing
the refreshed data/analytics.db is how the deployed app's data updates.
"""

import os
from pathlib import Path
from typing import Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text

load_dotenv()

SQLITE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "analytics.db"

# Court coordinates (feet), basket center at (0, 5.25), baseline at y=0.
# One or more marker positions per zone, used to plot bubbles on the
# half-court diagram.
ZONE_MARKERS = {
    "Restricted Area": [(0, 3)],
    "In The Paint (Non-RA)": [(0, 13)],
    "Mid-Range": [(-14, 17), (14, 17)],
    "Left Corner 3": [(-23.5, 3)],
    "Right Corner 3": [(23.5, 3)],
    "Above the Break 3": [(-16, 27), (0, 31), (16, 27)],
}

THREE_PT_ZONES = ("Left Corner 3", "Right Corner 3", "Above the Break 3")


def _get_engine():
    if os.getenv("DB_HOST"):
        url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER', 'nba_user')}:"
            f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:"
            f"{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'nba_pipeline')}"
        )
        return create_engine(url)
    return create_engine(f"sqlite:///{SQLITE_PATH}")


def headshot_url(player_id: int) -> str:
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"


def logo_url(team_id: int) -> str:
    return f"https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"


# ---------- Module 1: shooting zone analysis ----------

@st.cache_data(ttl=3600)
def get_shooting_zone_players() -> pd.DataFrame:
    """Every qualifying player-season, for the player dropdowns."""
    engine = _get_engine()
    df = pd.read_sql(text("""
        SELECT DISTINCT psz.player_id, psz.player_name, psz.team_id, psz.season, t.team_abbr
        FROM player_shooting_zones psz
        JOIN teams t ON t.team_id = psz.team_id
        ORDER BY psz.season DESC, psz.player_name
    """), engine)
    df["label"] = df["player_name"] + " (" + df["season"] + ")"
    df["headshot"] = df["player_id"].map(headshot_url)
    df["logo"] = df["team_id"].map(logo_url)
    return df


@st.cache_data(ttl=3600)
def get_shot_zones(player_id: int, season: str) -> pd.DataFrame:
    """
    Zone-level attempt share and FG% for one player-season vs league average,
    plus each zone's FG% percentile rank against every other qualifying
    player in that zone/season (PERCENT_RANK, works the same on Postgres
    and SQLite).
    """
    engine = _get_engine()
    df = pd.read_sql(text("""
        SELECT zone, fga, fgm, fg_pct, zone_frequency, league_fg_pct, league_attempt_share
        FROM player_shooting_zones
        WHERE player_id = :player_id AND season = :season
    """), engine, params={"player_id": int(player_id), "season": season})
    df["attempt_pct"] = (df["zone_frequency"] * 100).round(1)
    df["fg_pct"] = (df["fg_pct"] * 100).round(1)
    df["league_fg_pct"] = (df["league_fg_pct"] * 100).round(1)
    df["league_attempt_pct"] = (df["league_attempt_share"] * 100).round(1)
    df["fg_pct_delta"] = (df["fg_pct"] - df["league_fg_pct"]).round(1)
    df["markers"] = df["zone"].map(ZONE_MARKERS)

    pctile = pd.read_sql(text("""
        WITH ranked AS (
            SELECT player_id, zone,
                   PERCENT_RANK() OVER (PARTITION BY zone ORDER BY fg_pct) AS percentile
            FROM player_shooting_zones
            WHERE season = :season AND fg_pct IS NOT NULL
        )
        SELECT zone, percentile FROM ranked WHERE player_id = :player_id
    """), engine, params={"player_id": int(player_id), "season": season})
    pctile["percentile"] = (pctile["percentile"] * 100).round(0).astype(int)
    df = df.merge(pctile, on="zone", how="left")
    return df


@st.cache_data(ttl=3600)
def get_zone_fg_pct_range() -> Tuple[float, float]:
    """5th/95th percentile of zone fg_pct across the whole table, for the color scale."""
    engine = _get_engine()
    df = pd.read_sql(text("SELECT fg_pct FROM player_shooting_zones WHERE fg_pct IS NOT NULL"), engine)
    pct = df["fg_pct"] * 100
    return float(pct.quantile(0.05)), float(pct.quantile(0.95))


@st.cache_data(ttl=3600)
def get_three_pt_trend(player_id: int) -> pd.DataFrame:
    """Season-by-season 3-point attempt rate, for players qualifying in more than one season."""
    engine = _get_engine()
    query = text("""
        SELECT season, SUM(zone_frequency) AS three_pt_rate
        FROM player_shooting_zones
        WHERE player_id = :player_id AND zone IN :zones
        GROUP BY season
        ORDER BY season
    """).bindparams(bindparam("zones", expanding=True))
    df = pd.read_sql(query, engine, params={"player_id": int(player_id), "zones": list(THREE_PT_ZONES)})
    df["three_pt_rate"] = (df["three_pt_rate"] * 100).round(1)
    return df


# ---------- Module 2: playoff upset tracking ----------

@st.cache_data(ttl=3600)
def get_playoff_upsets() -> pd.DataFrame:
    engine = _get_engine()
    df = pd.read_sql(text("""
        SELECT pu.season, t1.conference, t1.team_abbr AS seed1_team, t1.team_id AS seed1_team_id,
               t2.team_abbr AS opponent, t2.team_id AS opponent_team_id,
               pu.round, pu.series_result, pu.upset_margin
        FROM playoff_upsets pu
        JOIN teams t1 ON t1.team_id = pu.higher_seed_team_id
        JOIN teams t2 ON t2.team_id = pu.lower_seed_team_id
    """), engine)
    df["upset"] = df["upset_margin"] > 0
    df["seed1_logo"] = df["seed1_team_id"].map(logo_url)
    df["opponent_logo"] = df["opponent_team_id"].map(logo_url)

    games = df["series_result"].apply(lambda s: sum(int(x) for x in s.split("-")))
    df["went_the_distance"] = games == 7

    def _result(row) -> str:
        wins, losses = (int(x) for x in row["series_result"].split("-"))
        if row["upset"]:
            return f"Eliminated in {row['round']}, {losses}-{wins}"
        return f"Reached Conference Finals ({wins}-{losses})"

    df["result"] = df.apply(_result, axis=1)
    return df


def get_upset_summary(df: pd.DataFrame) -> dict:
    total = len(df)
    upsets = int(df["upset"].sum())
    return {"total": total, "upsets": upsets, "rate": round(upsets / total * 100, 1) if total else 0.0}


def get_upsetting_teams(df: pd.DataFrame) -> pd.DataFrame:
    """Which teams knocked a #1 seed out before the conference finals, and how often."""
    upsets = df[df["upset"]]
    counts = (
        upsets.groupby(["opponent", "opponent_logo"])
        .size()
        .reset_index(name="upsets")
        .sort_values("upsets", ascending=False)
    )
    return counts


def get_upset_rate_by_season(df: pd.DataFrame) -> pd.DataFrame:
    """Upset rate per season, both conferences combined."""
    rate = df.groupby("season")["upset"].mean().mul(100).round(1).reset_index()
    return rate.sort_values("season")


# ---------- Module 3: MVP profile analysis ----------

@st.cache_data(ttl=3600)
def get_mvp_profiles() -> pd.DataFrame:
    engine = _get_engine()
    df = pd.read_sql(text("""
        SELECT p.season, p.player_id, p.player_name AS name,
               w.team_id, t.team_abbr AS team,
               p.points_per_game AS pts, p.rebounds_per_game AS reb, p.assists_per_game AS ast,
               p.ts_pct, p.team_win_pct AS win_pct,
               p.league_avg_pts, p.league_avg_reb, p.league_avg_ast, p.league_avg_ts_pct
        FROM mvp_season_profiles p
        JOIN mvp_winners w ON w.season = p.season AND w.player_id = p.player_id
        JOIN teams t ON t.team_id = w.team_id
        ORDER BY p.season DESC
    """), engine)
    for col in ("ts_pct", "win_pct", "league_avg_ts_pct"):
        df[col] = (df[col] * 100).round(1)
    df["headshot"] = df["player_id"].map(headshot_url)
    df["logo"] = df["team_id"].map(logo_url)

    # Percentile among the 10 MVP seasons themselves -- there's no raw
    # per-player data for the full league beyond the season averages, so
    # "percentile among all rotation players" isn't something this data can
    # answer. Ranking among fellow MVP seasons is the honest version of the
    # same idea: how this MVP's numbers stack up against the other 9.
    for col in ("pts", "reb", "ast", "ts_pct", "win_pct"):
        df[f"{col}_percentile"] = (df[col].rank(pct=True) * 100).round(0).astype(int)

    return df
