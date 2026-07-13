-- ============================================
-- Migration 003: Playoff Game Logs
-- Safe to run against existing nba_pipeline DB.
-- ============================================

BEGIN;

CREATE TABLE IF NOT EXISTS raw_playoff_games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) NOT NULL,
    season VARCHAR(10) NOT NULL,
    game_date DATE NOT NULL,
    team_id INTEGER NOT NULL,
    opponent_team_id INTEGER,
    matchup VARCHAR(20),
    win_loss VARCHAR(1),
    pts INTEGER,
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_playoff_games_season ON raw_playoff_games(season);
CREATE INDEX IF NOT EXISTS idx_raw_playoff_games_team ON raw_playoff_games(team_id);

COMMIT;
