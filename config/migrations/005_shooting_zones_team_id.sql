-- ============================================
-- Migration 005: Add team_id to player_shooting_zones
-- Safe to run against existing nba_pipeline DB.
-- Needed for the dashboard to show a team logo next
-- to a player without a second lookup; team_id was
-- already being fetched (buried in raw_shot_zone_splits'
-- raw_data) but never carried into the analytics table.
-- ============================================

BEGIN;

ALTER TABLE player_shooting_zones ADD COLUMN IF NOT EXISTS team_id INTEGER REFERENCES teams(team_id);

COMMIT;
