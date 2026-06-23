"""
NBA Analytics Platform — Streamlit dashboard (demo build)
Three modules: shooting zones, playoff upset tracking, MVP profiles.
Runs entirely on mock/sample data from data_generator.py — no live
pipeline connection yet. Real player headshots and team logos are
hot-linked from NBA.com's public CDN for visual realism.
"""

import numpy as np
import plotly.graph_objects as go
import streamlit as st

import data_generator as dg

st.set_page_config(page_title="NBA Analytics Platform", page_icon="🏀", layout="wide")

INK = "#10141C"
PAPER = "#FFFFFF"
BG = "#F4F5F7"
MUTED = "#6B7280"
BORDER = "#E5E7EB"
ORANGE = "#E2632B"
COURT_GREEN = "#1B4332"
GOLD = "#C9A227"
BLUE = "#3B82C4"
RED = "#DC2626"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Inter:wght@400;500;600&family=IBM+Plex+Mono:wght@500;600&display=swap');
.stApp {{ background-color: {BG}; }}
.block-container {{ padding-top: 1.2rem; max-width: 1180px; }}
.js-plotly-plot, .plotly, .main-svg {{ touch-action: pan-y !important; }}
html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
h1, h2, h3 {{ font-family: 'Space Grotesk', sans-serif !important; color: {INK} !important; }}
.stat-num {{ font-family: 'IBM Plex Mono', monospace; font-weight: 600; }}
.nba-header {{
    background: {INK}; border-radius: 14px; padding: 18px 24px;
    display: flex; align-items: center; justify-content: space-between;
    flex-wrap: wrap; gap: 10px; margin-bottom: 22px;
}}
.nba-header .eyebrow {{ color: #9CA3AF; font-size: 12px; font-weight: 600; letter-spacing: .08em; }}
.nba-header .htitle {{ color: white; font-family: 'Space Grotesk', sans-serif; font-size: 22px; font-weight: 700; margin-top: 2px; }}
.pill {{
    background: rgba(255,255,255,0.08); color: #E5E7EB; border-radius: 999px;
    padding: 8px 16px; font-size: 13px; font-weight: 500; white-space: nowrap;
}}
.nba-card {{
    background: {PAPER}; border: 1px solid {BORDER}; border-radius: 14px;
    padding: 20px 22px; margin-bottom: 14px;
}}
.zone-label {{ color: {MUTED}; font-size: 12px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }}
.player-row {{ display: flex; align-items: center; gap: 14px; }}
.player-row img.headshot {{ width: 64px; height: 64px; border-radius: 50%; object-fit: cover; border: 2px solid {BORDER}; background: #f0f0f0; }}
img.logo {{ width: 30px; height: 30px; object-fit: contain; flex-shrink: 0; }}
.player-name {{ font-family: 'Space Grotesk', sans-serif; font-weight: 700; font-size: 18px; color: {INK}; }}
.player-meta {{ color: {MUTED}; font-size: 13px; }}
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="nba-header">
  <div>
    <div class="eyebrow">DEMO BUILD · MOCK DATA</div>
    <div class="htitle">NBA Analytics Platform</div>
  </div>
  <div style="display:flex; gap:10px; flex-wrap:wrap;">
    <div class="pill">3 analytics modules</div>
    <div class="pill">Sample / mock data</div>
    <div class="pill">Real headshots + logos</div>
  </div>
</div>
""", unsafe_allow_html=True)

st.caption(
    "This build runs on hand-crafted sample data so the dashboard can be previewed end to end. "
    "It will be refined and connected to the real Airflow/Postgres pipeline once ingestion and "
    "analytics ETL are complete — see PROJECT_STATUS.md."
)

def show_chart(fig):
    """Render a Plotly figure with touch-drag interactions disabled, so
    accidental finger contact while scrolling on mobile doesn't trigger
    zoom/pan/select on the chart."""
    fig.update_layout(dragmode=False)
    st.plotly_chart(
        fig, use_container_width=True,
        config={"scrollZoom": False, "displayModeBar": False, "doubleClick": False},
    )


tab1, tab2, tab3 = st.tabs(["Shooting Zones", "Playoff Upsets", "MVP Profiles"])


def draw_half_court(zone_df) -> go.Figure:
    fig = go.Figure()

    # --- court lines ---
    court_lines = [
        ([-25, 25], [0, 0]),                 # baseline
        ([-25, -25], [0, 35]),               # left sideline
        ([25, 25], [0, 35]),                 # right sideline
        ([-3, 3], [4, 4]),                   # backboard
        ([-8, -8, 8, 8, -8], [0, 19, 19, 0, 0]),   # free-throw lane
        ([-4, -4], [0, 5.25]), ([4, 4], [0, 5.25]),  # RA verticals
    ]
    for x, y in court_lines:
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines", line=dict(color="#C7CCD1", width=1.5),
                                  hoverinfo="skip", showlegend=False))

    t = np.linspace(0, 2 * np.pi, 60)
    fig.add_trace(go.Scatter(x=6 * np.cos(t), y=19 + 6 * np.sin(t), mode="lines",
                              line=dict(color="#C7CCD1", width=1.5), hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Scatter(x=0.75 * np.cos(t), y=5.25 + 0.75 * np.sin(t), mode="lines",
                              line=dict(color=ORANGE, width=2), hoverinfo="skip", showlegend=False))

    ra_t = np.linspace(0, np.pi, 30)
    fig.add_trace(go.Scatter(x=4 * np.cos(ra_t), y=5.25 + 4 * np.sin(ra_t), mode="lines",
                              line=dict(color="#C7CCD1", width=1.5), hoverinfo="skip", showlegend=False))

    t0 = np.arcsin(22 / 23.75)
    arc_t = np.linspace(-t0, t0, 60)
    arc_x = 23.75 * np.sin(arc_t)
    arc_y = 5.25 + 23.75 * np.cos(arc_t)
    fig.add_trace(go.Scatter(
        x=np.concatenate([[-22], arc_x, [22]]),
        y=np.concatenate([[0], arc_y, [0]]),
        mode="lines", line=dict(color="#C7CCD1", width=1.5), hoverinfo="skip", showlegend=False,
    ))

    # --- zone bubbles ---
    mx, my, msize, mcolor, mtext = [], [], [], [], []
    for _, row in zone_df.iterrows():
        markers = row["markers"]
        share = row["attempt_pct"] / len(markers)
        for (x, y) in markers:
            mx.append(x); my.append(y)
            msize.append(18 + share * 2.6)
            mcolor.append(row["fg_pct"])
            mtext.append(f"{row['zone']}<br>{row['attempt_pct']}% of shots · {row['fg_pct']}% FG"
                          f"<br>League avg: {row['league_fg_pct']}% FG")

    fig.add_trace(go.Scatter(
        x=mx, y=my, mode="markers", text=mtext, hovertemplate="%{text}<extra></extra>",
        marker=dict(size=msize, color=mcolor, colorscale=[[0, BLUE], [0.5, "#F2C14E"], [1, ORANGE]],
                    cmin=25, cmax=75, line=dict(width=1, color="white"),
                    colorbar=dict(title="FG%", thickness=12, len=0.6)),
        showlegend=False,
    ))

    fig.update_layout(
        plot_bgcolor="#FBF7EF", paper_bgcolor=PAPER, height=440,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(visible=False, range=[-27, 27]),
        yaxis=dict(visible=False, range=[-2, 36], scaleanchor="x", scaleratio=1),
    )
    return fig


# ================= TAB 1: SHOOTING ZONES =================
with tab1:
    st.markdown("### Player Shooting Zone Analysis")
    st.caption("Shot distribution and efficiency by court zone, compared to league average.")

    players = dg.get_players()
    names = [p["name"] for p in players]
    choice = st.selectbox("Player", names, label_visibility="collapsed")
    player = next(p for p in players if p["name"] == choice)
    zones = dg.get_shot_zones(choice)

    col_card, col_court = st.columns([1, 1.6])
    with col_card:
        st.markdown(f"""
        <div class="nba-card">
          <div class="player-row">
            <img class="headshot" src="{player['headshot']}">
            <div>
              <div class="player-name">{player['name']}</div>
              <div class="player-meta">{player['pos']} · {player['team']}</div>
            </div>
            <img class="logo" src="{player['logo']}" style="margin-left:auto;">
          </div>
        </div>
        """, unsafe_allow_html=True)

        top_zone = zones.sort_values("attempt_pct", ascending=False).iloc[0]
        best_fg = zones.sort_values("fg_pct", ascending=False).iloc[0]
        st.markdown(f"""
        <div class="nba-card">
          <div class="zone-label">Primary shot zone</div>
          <div class="stat-num" style="font-size:20px;color:{INK};">{top_zone['zone']}</div>
          <div class="player-meta">{top_zone['attempt_pct']}% of shot attempts</div>
          <div style="height:10px;"></div>
          <div class="zone-label">Most efficient zone</div>
          <div class="stat-num" style="font-size:20px;color:{INK};">{best_fg['zone']}</div>
          <div class="player-meta">{best_fg['fg_pct']}% FG · league avg {best_fg['league_fg_pct']}%</div>
        </div>
        """, unsafe_allow_html=True)

    with col_court:
        show_chart(draw_half_court(zones))
        st.caption("Bubble size = share of shot attempts · color = field goal % (blue low → orange high)")

    st.markdown("#### Zone efficiency vs. league average")
    fig = go.Figure()
    fig.add_trace(go.Bar(x=zones["zone"], y=zones["fg_pct"], name=choice, marker_color=ORANGE))
    fig.add_trace(go.Bar(x=zones["zone"], y=zones["league_fg_pct"], name="League avg", marker_color="#D1D5DB"))
    fig.update_layout(
        barmode="group", height=320, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
        margin=dict(l=10, r=10, t=10, b=10), font=dict(color=INK, family="Inter"),
        yaxis=dict(title="FG%", gridcolor="#EEE"), legend=dict(orientation="h", y=1.1),
    )
    show_chart(fig)


# ================= TAB 2: PLAYOFF UPSETS =================
with tab2:
    st.markdown("### Playoff Upset Tracking")
    st.caption("How often #1 seeds get eliminated in the first round, and by whom. Sample data — "
               "seasons and matchups below are randomly generated, not actual historical results.")

    upsets = dg.get_playoff_upsets(10)
    summary = dg.get_upset_summary(upsets)

    c1, c2, c3 = st.columns(3)
    for col, label, value in [
        (c1, "#1 seeds tracked", summary["total"]),
        (c2, "Eliminated in round 1", summary["upsets"]),
        (c3, "Upset rate", f"{summary['rate']}%"),
    ]:
        col.markdown(f"""
        <div class="nba-card">
          <div class="zone-label">{label}</div>
          <div class="stat-num" style="font-size:26px;color:{INK};">{value}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("#### Season-by-season")
    for _, row in upsets.sort_values("season", ascending=False).iterrows():
        badge_color = RED if row["upset"] else "#16A34A"
        badge_text = "UPSET" if row["upset"] else "ADVANCED"
        st.markdown(f"""
        <div class="nba-card" style="display:flex; align-items:center; gap:18px;">
          <div style="width:60px; color:{MUTED}; font-size:13px; font-weight:600;">{row['season']}</div>
          <div style="width:60px; color:{MUTED}; font-size:13px;">{row['conference']}</div>
          <img class="logo" src="{row['seed1_logo']}">
          <div style="font-weight:600; color:{INK};">{row['seed1_team']}</div>
          <div style="color:{MUTED};">vs</div>
          <img class="logo" src="{row['opponent_logo']}">
          <div style="font-weight:600; color:{INK};">{row['opponent']}</div>
          <div style="margin-left:auto; color:{MUTED}; font-size:13px;">{row['result']}</div>
          <div style="background:{badge_color}1A; color:{badge_color}; padding:4px 12px;
                      border-radius:999px; font-size:12px; font-weight:700;">{badge_text}</div>
        </div>
        """, unsafe_allow_html=True)

    by_conf = upsets.groupby("conference")["upset"].mean().mul(100).round(1).reset_index()
    fig = go.Figure(go.Bar(x=by_conf["conference"], y=by_conf["upset"], marker_color=[BLUE, ORANGE],
                            text=by_conf["upset"].map(lambda v: f"{v}%"), textposition="outside"))
    fig.update_layout(height=280, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
                       margin=dict(l=10, r=10, t=30, b=10), font=dict(color=INK, family="Inter"),
                       yaxis=dict(title="Upset rate (%)", gridcolor="#EEE"))
    st.markdown("#### Upset rate by conference")
    show_chart(fig)


# ================= TAB 3: MVP PROFILES =================
with tab3:
    st.markdown("### MVP Profile Analysis")
    st.caption("Statistical profile of MVP winners across the last ten seasons. Sample data — "
               "stat lines are illustrative approximations for this demo, not verified records.")

    mvps = dg.get_mvp_profiles()
    latest = mvps.iloc[0]

    col_card, col_radar = st.columns([1, 1.4])
    with col_card:
        st.markdown(f"""
        <div class="nba-card">
          <div class="zone-label">Most recent MVP</div>
          <div class="player-row" style="margin-top:8px;">
            <img class="headshot" src="{latest['headshot']}">
            <div>
              <div class="player-name">{latest['name']}</div>
              <div class="player-meta">{latest['team']} · {latest['season']}</div>
            </div>
            <img class="logo" src="{latest['logo']}" style="margin-left:auto;">
          </div>
          <div style="height:14px;"></div>
          <div class="zone-label">Stat line</div>
          <div class="stat-num" style="font-size:18px;color:{INK};">
            {latest['pts']} PTS · {latest['reb']} REB · {latest['ast']} AST
          </div>
          <div class="player-meta">{latest['ts_pct']}% TS · team win rate {latest['win_pct']}%</div>
        </div>
        """, unsafe_allow_html=True)

    with col_radar:
        labels = ["PTS", "REB", "AST", "TS%", "Win%"]
        mins = np.array([20, 3, 3, 50, 50])
        maxs = np.array([35, 15, 11, 70, 85])
        fig = go.Figure()
        for _, row in mvps.head(3).iterrows():
            raw = np.array([row["pts"], row["reb"], row["ast"], row["ts_pct"], row["win_pct"]])
            norm = ((raw - mins) / (maxs - mins) * 100).round(0)
            fig.add_trace(go.Scatterpolar(
                r=list(norm) + [norm[0]], theta=labels + [labels[0]], fill="toself",
                name=f"{row['name'].split()[-1]} ({row['season']})",
            ))
        fig.update_layout(
            polar=dict(bgcolor=PAPER, radialaxis=dict(visible=True, range=[0, 100], showticklabels=False)),
            paper_bgcolor=PAPER, height=360, margin=dict(l=30, r=30, t=20, b=20),
            legend=dict(orientation="h", y=-0.1), font=dict(family="Inter", color=INK),
        )
        show_chart(fig)
        st.caption("Last 3 MVP seasons, stats normalized 0-100 for comparison.")

    st.markdown("#### Team win rate vs. scoring, last 10 MVP seasons")
    fig2 = go.Figure(go.Scatter(
        x=mvps["pts"], y=mvps["win_pct"], mode="markers+text",
        text=mvps["season"], textposition="top center",
        marker=dict(size=14, color=GOLD, line=dict(width=1, color="white")),
    ))
    fig2.update_layout(
        height=320, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
        margin=dict(l=10, r=10, t=10, b=10), font=dict(color=INK, family="Inter"),
        xaxis=dict(title="Points per game", gridcolor="#EEE"),
        yaxis=dict(title="Team win rate (%)", gridcolor="#EEE"),
    )
    show_chart(fig2)

st.divider()
st.caption(
    "Demo build on mock/sample data. Player headshots and team logos are real, hot-linked from "
    "NBA.com's public CDN — everything else will be replaced by the live Airflow/Postgres "
    "pipeline once ingestion and analytics ETL are complete."
)
