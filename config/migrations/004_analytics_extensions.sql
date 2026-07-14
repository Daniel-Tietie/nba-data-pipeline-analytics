-- ============================================
-- Migration 004: Analytics ETL support
-- Safe to run against existing nba_pipeline DB.
-- Adds league-average context columns needed by
-- the Phase 2 analytics builders, and a raw table
-- to source league-wide per-season stats from.
-- ============================================

BEGIN;

CREATE TABLE IF NOT EXISTS raw_league_season_averages (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL UNIQUE,
    qualified_players INTEGER,
    avg_pts DECIMAL(5,1),
    avg_reb DECIMAL(5,1),
    avg_ast DECIMAL(5,1),
    avg_fga DECIMAL(5,1),
    avg_fta DECIMAL(5,1),
    avg_fg_pct DECIMAL(5,3),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE mvp_season_profiles ADD COLUMN IF NOT EXISTS ts_pct DECIMAL(5,3);
ALTER TABLE mvp_season_profiles ADD COLUMN IF NOT EXISTS team_win_pct DECIMAL(5,3);
ALTER TABLE mvp_season_profiles ADD COLUMN IF NOT EXISTS league_avg_pts DECIMAL(5,1);
ALTER TABLE mvp_season_profiles ADD COLUMN IF NOT EXISTS league_avg_reb DECIMAL(5,1);
ALTER TABLE mvp_season_profiles ADD COLUMN IF NOT EXISTS league_avg_ast DECIMAL(5,1);
ALTER TABLE mvp_season_profiles ADD COLUMN IF NOT EXISTS league_avg_ts_pct DECIMAL(5,3);

ALTER TABLE player_shooting_zones ADD COLUMN IF NOT EXISTS league_fg_pct DECIMAL(5,3);
ALTER TABLE player_shooting_zones ADD COLUMN IF NOT EXISTS league_attempt_share DECIMAL(5,3);

COMMIT;
