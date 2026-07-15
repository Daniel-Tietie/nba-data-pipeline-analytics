"""
NBA Analytics Platform — Streamlit dashboard
Three modules: shooting zones, playoff upset tracking, MVP profiles.
Reads from queries.py, which hits Postgres locally or a bundled SQLite
snapshot when deployed. Player headshots and team logos are hot-linked
from NBA.com's public CDN.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import queries as q

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
.block-container {{ padding-top: 3rem; max-width: 1180px; }}
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
    <div class="eyebrow">LIVE PIPELINE · 2015-16 TO 2024-25</div>
    <div class="htitle">NBA Analytics Platform</div>
  </div>
  <div style="display:flex; gap:10px; flex-wrap:wrap;">
    <div class="pill">3 analytics modules</div>
    <div class="pill">Real pipeline data</div>
    <div class="pill">Real headshots + logos</div>
  </div>
</div>
""", unsafe_allow_html=True)

st.caption(
    "Data comes from an Airflow pipeline that pulls the NBA stats API, lands it in Postgres, "
    "and rebuilds these three analytics tables on a schedule. This deployed copy reads a snapshot "
    "exported from that pipeline, not a live database connection -- see the README for how it's wired."
)

def show_chart(fig, key=None):
    """Render a Plotly figure with touch-drag interactions disabled, so
    accidental finger contact while scrolling on mobile doesn't trigger
    zoom/pan/select on the chart."""
    fig.update_layout(dragmode=False)
    st.plotly_chart(
        fig, use_container_width=True, key=key,
        config={"scrollZoom": False, "displayModeBar": False, "doubleClick": False},
    )


tab1, tab2, tab3 = st.tabs(["Shooting Zones", "Playoff Upsets", "MVP Profiles"])


def draw_half_court(zone_df, cmin, cmax) -> go.Figure:
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
                          f"<br>League avg: {row['league_fg_pct']}% FG · {row['percentile']}th percentile")

    fig.add_trace(go.Scatter(
        x=mx, y=my, mode="markers", text=mtext, hovertemplate="%{text}<extra></extra>",
        marker=dict(size=msize, color=mcolor, colorscale=[[0, BLUE], [0.5, "#F2C14E"], [1, ORANGE]],
                    cmin=cmin, cmax=cmax, line=dict(width=1, color="white"),
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
    st.caption("Shot distribution and efficiency by court zone, compared to league average. "
               "Players need at least 200 field goal attempts in a season to show up here. "
               "Percentile = the share of every qualifying player-season in that zone they out-shot.")

    players = q.get_shooting_zone_players()
    cmin, cmax = q.get_zone_fg_pct_range()

    col_p1, col_p2 = st.columns(2)
    with col_p1:
        choice_a = st.selectbox("Player", players["label"], label_visibility="collapsed")
    with col_p2:
        compare_options = ["Compare to another player..."] + list(players["label"])
        choice_b = st.selectbox("Compare", compare_options, label_visibility="collapsed")

    player_a = players[players["label"] == choice_a].iloc[0]
    zones_a = q.get_shot_zones(player_a["player_id"], player_a["season"])
    comparing = choice_b != "Compare to another player..."

    def player_card(player, zones):
        st.markdown(f"""
        <div class="nba-card">
          <div class="player-row">
            <img class="headshot" src="{player['headshot']}">
            <div>
              <div class="player-name">{player['player_name']}</div>
              <div class="player-meta">{player['team_abbr']} · {player['season']}</div>
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
          <div class="player-meta">{best_fg['fg_pct']}% FG · league avg {best_fg['league_fg_pct']}% · {best_fg['percentile']}th percentile</div>
        </div>
        """, unsafe_allow_html=True)

    if comparing:
        player_b = players[players["label"] == choice_b].iloc[0]
        zones_b = q.get_shot_zones(player_b["player_id"], player_b["season"])

        col_a, col_b = st.columns(2)
        with col_a:
            player_card(player_a, zones_a)
        with col_b:
            player_card(player_b, zones_b)

        st.markdown(f"#### {player_a['player_name']} vs. {player_b['player_name']}")
        col_court_a, col_court_b = st.columns(2)
        with col_court_a:
            st.caption(f"{player_a['player_name']} ({player_a['season']})")
            show_chart(draw_half_court(zones_a, cmin, cmax), key="court_a")
        with col_court_b:
            st.caption(f"{player_b['player_name']} ({player_b['season']})")
            show_chart(draw_half_court(zones_b, cmin, cmax), key="court_b")

        st.markdown("#### Zone efficiency, head to head")
        fig = go.Figure()
        for zones, player, color in [(zones_a, player_a, ORANGE), (zones_b, player_b, BLUE)]:
            fig.add_trace(go.Bar(
                x=zones["zone"], y=zones["fg_pct"], name=player["player_name"],
                marker=dict(color=color, cornerradius=4), width=0.35,
                text=zones["fg_pct"].map(lambda v: f"{v:.0f}%"), textposition="outside",
                textfont=dict(color=INK, size=11),
                hovertemplate="%{x}<br>%{y:.1f}% FG<extra></extra>",
            ))
        fig.update_layout(barmode="group", bargap=0.35, bargroupgap=0.15)
        chart_max = max(zones_a["fg_pct"].max(), zones_b["fg_pct"].max())
    else:
        col_card, col_court = st.columns([1, 1.6])
        with col_card:
            player_card(player_a, zones_a)
        with col_court:
            show_chart(draw_half_court(zones_a, cmin, cmax), key="court_single")
            st.caption("Bubble size = share of shot attempts · color = field goal % (blue low → orange high)")

        st.markdown("#### Zone efficiency vs. league average")
        st.caption("Orange bars beat the league average in that zone, blue bars fall short. "
                   "The gray tick marks where league average sits.")
        above = zones_a["fg_pct"] >= zones_a["league_fg_pct"]
        y_above = zones_a["fg_pct"].where(above)
        y_below = zones_a["fg_pct"].where(~above)

        fig = go.Figure()
        for y, name, color in [(y_above, "Above avg", ORANGE), (y_below, "Below avg", BLUE)]:
            fig.add_trace(go.Bar(
                x=zones_a["zone"], y=y, name=name,
                marker=dict(color=color, cornerradius=4), width=0.42,
                text=y.map(lambda v: f"{v:.0f}%" if pd.notna(v) else ""), textposition="outside",
                textfont=dict(color=INK, size=11),
                hovertemplate="%{x}<br>%{y:.1f}% FG<extra></extra>",
            ))
        fig.add_trace(go.Scatter(
            x=zones_a["zone"], y=zones_a["league_fg_pct"], name="League avg",
            mode="markers", marker=dict(symbol="line-ew", size=26, line=dict(width=3, color=MUTED)),
            hovertemplate="%{x}<br>League avg %{y:.1f}% FG<extra></extra>",
        ))
        fig.update_layout(bargap=0.3)
        chart_max = max(zones_a["fg_pct"].max(), zones_a["league_fg_pct"].max())

    fig.update_layout(
        height=380, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
        margin=dict(l=10, r=10, t=90, b=10), font=dict(color=INK, family="Inter"),
        yaxis=dict(title="FG%", gridcolor="#EEE", gridwidth=1, zeroline=False, range=[0, chart_max * 1.18]),
        xaxis=dict(showgrid=False),
        legend=dict(orientation="h", y=1.25, x=0),
    )
    show_chart(fig, key="zone_efficiency_bar")

    st.markdown("#### Shot selection quality")
    st.caption(f"Each dot is one of {player_a['player_name']}'s six zones: how often they shoot from "
               "there (left to right) against how much better or worse than league average they shoot "
               "from there (bottom to top). Top-right is the sweet spot -- high volume and above "
               "average. Bottom-right is a real weakness: shoots a lot from there without making it "
               "at an average rate.")
    fig_quad = go.Figure()
    avg_attempt_share = 100 / 6
    fig_quad.add_hline(y=0, line=dict(color="#D1D5DB", width=1))
    fig_quad.add_vline(x=avg_attempt_share, line=dict(color="#D1D5DB", width=1, dash="dot"))
    quad_colors = zones_a["fg_pct_delta"].map(lambda v: ORANGE if v >= 0 else BLUE)
    fig_quad.add_trace(go.Scatter(
        x=zones_a["attempt_pct"], y=zones_a["fg_pct_delta"], mode="markers+text",
        text=zones_a["zone"], textposition="top center", textfont=dict(size=10, color=MUTED),
        marker=dict(size=16, color=quad_colors, line=dict(width=2, color=PAPER)),
        customdata=zones_a["fg_pct"],
        hovertemplate="%{text}<br>%{x:.1f}% of shots<br>%{y:+.1f} pts vs league avg (%{customdata:.1f}% FG)<extra></extra>",
        showlegend=False,
    ))
    fig_quad.update_layout(
        height=320, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
        margin=dict(l=10, r=10, t=20, b=10), font=dict(color=INK, family="Inter"),
        xaxis=dict(title="Share of shot attempts", gridcolor="#EEE", zeroline=False),
        yaxis=dict(title="FG% vs. league average", gridcolor="#EEE", zeroline=False),
    )
    show_chart(fig_quad, key="shot_quality_quadrant")

    trend = q.get_three_pt_trend(player_a["player_id"])
    if len(trend) > 1:
        st.markdown(f"#### {player_a['player_name']}'s 3-point rate over time")
        st.caption("Share of shot attempts from three-point range, across the seasons this player "
                   "has qualifying data for.")
        fig_trend = go.Figure(go.Scatter(
            x=trend["season"], y=trend["three_pt_rate"], mode="lines+markers+text",
            text=trend["three_pt_rate"].map(lambda v: f"{v:.0f}%"), textposition="top center",
            line=dict(color=ORANGE, width=2), marker=dict(size=8, color=ORANGE),
        ))
        fig_trend.update_layout(
            height=240, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
            margin=dict(l=10, r=10, t=30, b=10), font=dict(color=INK, family="Inter"),
            yaxis=dict(title="3PT attempt rate", gridcolor="#EEE", zeroline=False),
            xaxis=dict(showgrid=False),
        )
        show_chart(fig_trend, key="three_pt_trend")


# ================= TAB 2: PLAYOFF UPSETS =================
with tab2:
    st.markdown("### Playoff Upset Tracking")
    st.caption("How often #1 seeds fail to reach the conference finals, and by whom. "
               "An upset here means a loss in round 1 or round 2 -- covers the 10 seasons "
               "from 2015-16 to 2024-25.")

    upsets = q.get_playoff_upsets()
    summary = q.get_upset_summary(upsets)

    c1, c2, c3 = st.columns(3)
    for col, label, value in [
        (c1, "#1 seeds tracked", summary["total"]),
        (c2, "Missed conference finals", summary["upsets"]),
        (c3, "Upset rate", f"{summary['rate']}%"),
    ]:
        col.markdown(f"""
        <div class="nba-card">
          <div class="zone-label">{label}</div>
          <div class="stat-num" style="font-size:26px;color:{INK};">{value}</div>
        </div>
        """, unsafe_allow_html=True)

    upsetting_teams = q.get_upsetting_teams(upsets)
    if not upsetting_teams.empty:
        st.markdown("#### Who did the upsetting")
        cols = st.columns(len(upsetting_teams))
        for col, (_, row) in zip(cols, upsetting_teams.iterrows()):
            col.markdown(f"""
            <div class="nba-card" style="text-align:center;">
              <img class="logo" src="{row['opponent_logo']}" style="width:36px;height:36px;">
              <div class="player-name" style="margin-top:6px;">{row['opponent']}</div>
              <div class="player-meta">{row['upsets']} upset{"s" if row["upsets"] != 1 else ""}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("#### Season-by-season")
    st.caption("The gold \"7 GAMES\" tag marks a series that went the distance, win or lose.")
    for _, row in upsets.sort_values("season", ascending=False).iterrows():
        badge_color = RED if row["upset"] else "#16A34A"
        badge_text = "UPSET" if row["upset"] else "ADVANCED"
        distance_tag = f"""
          <div style="background:{GOLD}1A; color:{GOLD}; padding:4px 12px;
                      border-radius:999px; font-size:12px; font-weight:700;">7 GAMES</div>
        """ if row["went_the_distance"] else ""
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
          {distance_tag}
          <div style="background:{badge_color}1A; color:{badge_color}; padding:4px 12px;
                      border-radius:999px; font-size:12px; font-weight:700;">{badge_text}</div>
        </div>
        """, unsafe_allow_html=True)

    col_conf, col_trend = st.columns(2)
    with col_conf:
        by_conf = upsets.groupby("conference")["upset"].mean().mul(100).round(1).reset_index()
        fig = go.Figure(go.Bar(
            x=by_conf["conference"], y=by_conf["upset"], marker=dict(color=[BLUE, ORANGE], cornerradius=4),
            width=0.5, text=by_conf["upset"].map(lambda v: f"{v}%"), textposition="outside",
            textfont=dict(color=INK, size=11),
        ))
        fig.update_layout(
            height=280, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
            margin=dict(l=10, r=10, t=30, b=10), font=dict(color=INK, family="Inter"),
            yaxis=dict(title="Upset rate (%)", gridcolor="#EEE", zeroline=False, range=[0, 110]),
            xaxis=dict(showgrid=False),
        )
        st.markdown("#### Upset rate by conference")
        show_chart(fig, key="upset_rate_by_conference")

    with col_trend:
        rate_by_season = q.get_upset_rate_by_season(upsets)
        fig_trend = go.Figure(go.Scatter(
            x=rate_by_season["season"], y=rate_by_season["upset"], mode="lines+markers",
            line=dict(color=RED, width=2), marker=dict(size=8, color=RED),
            hovertemplate="%{x}<br>%{y:.0f}% upset rate<extra></extra>",
        ))
        fig_trend.update_layout(
            height=280, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
            margin=dict(l=10, r=10, t=30, b=10), font=dict(color=INK, family="Inter"),
            yaxis=dict(title="Upset rate (%)", gridcolor="#EEE", zeroline=False, range=[0, 110]),
            xaxis=dict(showgrid=False),
        )
        st.markdown("#### Upset rate by season")
        show_chart(fig_trend, key="upset_rate_by_season")


# ================= TAB 3: MVP PROFILES =================
with tab3:
    st.markdown("### MVP Profile Analysis")
    st.caption("Statistical profile of MVP winners across the last ten seasons. Percentile = rank "
               "among the 10 MVP seasons themselves, not the full league -- the pipeline only stores "
               "full per-player stats for the MVPs, so that's the fair comparison this data can make.")

    mvps = q.get_mvp_profiles()
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        season_choice = st.selectbox("Season", mvps["season"], label_visibility="collapsed")
    with col_s2:
        other_seasons = [s for s in mvps["season"] if s != season_choice]
        compare_choice = st.selectbox("Compare to", ["League average"] + other_seasons, label_visibility="collapsed")

    selected = mvps[mvps["season"] == season_choice].iloc[0]

    col_card, col_radar = st.columns([1, 1.4])
    with col_card:
        st.markdown(f"""
        <div class="nba-card">
          <div class="zone-label">MVP</div>
          <div class="player-row" style="margin-top:8px;">
            <img class="headshot" src="{selected['headshot']}">
            <div>
              <div class="player-name">{selected['name']}</div>
              <div class="player-meta">{selected['team']} · {selected['season']}</div>
            </div>
            <img class="logo" src="{selected['logo']}" style="margin-left:auto;">
          </div>
          <div style="height:14px;"></div>
          <div class="zone-label">Stat line</div>
          <div class="stat-num" style="font-size:18px;color:{INK};">
            {selected['pts']} PTS · {selected['reb']} REB · {selected['ast']} AST
          </div>
          <div class="player-meta">{selected['ts_pct']}% TS · team win rate {selected['win_pct']}%</div>
          <div style="height:10px;"></div>
          <div class="zone-label">Percentile among the 10 MVP seasons</div>
          <div class="player-meta">
            PTS {selected['pts_percentile']}th · REB {selected['reb_percentile']}th ·
            AST {selected['ast_percentile']}th · TS% {selected['ts_pct_percentile']}th ·
            Win% {selected['win_pct_percentile']}th
          </div>
        </div>
        """, unsafe_allow_html=True)

    with col_radar:
        labels = ["PTS", "REB", "AST", "TS%", "Win%"]
        mins = np.array([15, 3, 3, 50, 40])
        maxs = np.array([35, 15, 12, 70, 90])

        mvp_raw = np.array([selected["pts"], selected["reb"], selected["ast"], selected["ts_pct"], selected["win_pct"]])

        if compare_choice == "League average":
            other_raw = np.array([
                selected["league_avg_pts"], selected["league_avg_reb"], selected["league_avg_ast"],
                selected["league_avg_ts_pct"], 50.0,
            ])
            other_name = "League average"
        else:
            other_row = mvps[mvps["season"] == compare_choice].iloc[0]
            other_raw = np.array([
                other_row["pts"], other_row["reb"], other_row["ast"], other_row["ts_pct"], other_row["win_pct"],
            ])
            other_name = f"{other_row['name']} ({other_row['season']})"

        fig = go.Figure()
        for raw, name, color in [(mvp_raw, f"{selected['name']} ({selected['season']})", ORANGE), (other_raw, other_name, "#9CA3AF" if compare_choice == "League average" else BLUE)]:
            norm = ((raw - mins) / (maxs - mins) * 100).round(0)
            fig.add_trace(go.Scatterpolar(
                r=list(norm) + [norm[0]], theta=labels + [labels[0]], fill="toself",
                name=name, line=dict(color=color),
            ))
        fig.update_layout(
            polar=dict(bgcolor=PAPER, radialaxis=dict(visible=True, range=[0, 100], showticklabels=False)),
            paper_bgcolor=PAPER, height=360, margin=dict(l=30, r=30, t=20, b=20),
            legend=dict(orientation="h", y=-0.1), font=dict(family="Inter", color=INK),
        )
        show_chart(fig, key="mvp_radar")
        st.caption(f"{selected['season']} MVP vs. {other_name.lower()}, normalized 0-100.")

    st.markdown("#### All 10 MVP seasons")
    table = mvps[["headshot", "season", "name", "team", "pts", "reb", "ast", "ts_pct", "win_pct"]].copy()
    table.columns = ["", "Season", "Player", "Team", "PTS", "REB", "AST", "TS%", "Win%"]
    st.dataframe(
        table,
        column_config={"": st.column_config.ImageColumn("", width="small")},
        hide_index=True, use_container_width=True,
    )

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
    show_chart(fig2, key="mvp_win_scatter")

    st.markdown("#### Scoring and efficiency across the era")
    st.caption("Both lines indexed to the 2015-16 MVP's numbers = 100, so scoring (points per game) "
               "and shooting efficiency (true shooting %) can sit on one scale even though they're "
               "measured completely differently. Above 100 means better than the 2015-16 MVP; below "
               "100 means worse.")
    era = mvps.sort_values("season").copy()
    era["pts_idx"] = (era["pts"] / era["pts"].iloc[0] * 100).round(1)
    era["ts_idx"] = (era["ts_pct"] / era["ts_pct"].iloc[0] * 100).round(1)
    fig_era = go.Figure()
    fig_era.add_hline(y=100, line=dict(color="#D1D5DB", width=1, dash="dot"))
    fig_era.add_trace(go.Scatter(x=era["season"], y=era["pts_idx"], mode="lines+markers",
                                  name="Scoring", line=dict(color=ORANGE, width=2)))
    fig_era.add_trace(go.Scatter(x=era["season"], y=era["ts_idx"], mode="lines+markers",
                                  name="Shooting efficiency", line=dict(color=BLUE, width=2)))
    fig_era.update_layout(
        height=280, plot_bgcolor=PAPER, paper_bgcolor=PAPER,
        margin=dict(l=10, r=10, t=30, b=10), font=dict(color=INK, family="Inter"),
        yaxis=dict(title="Indexed to 2015-16 = 100", gridcolor="#EEE", zeroline=False),
        xaxis=dict(showgrid=False), legend=dict(orientation="h", y=1.15),
    )
    show_chart(fig_era, key="mvp_era_trend")

st.divider()
st.caption(
    "Built on an Airflow/Postgres pipeline that ingests directly from the NBA stats API. "
    "Player headshots and team logos are hot-linked from NBA.com's public CDN."
)
