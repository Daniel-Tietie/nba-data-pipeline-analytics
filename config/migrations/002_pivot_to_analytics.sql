-- ============================================
-- Migration 002: Pivot to Analytics Platform
-- Safe to run against existing nba_pipeline DB.
-- Drops only unused ML tables; preserves
-- teams, games, team_stats, raw_*, dag_runs,
-- data_quality_checks and all their data.
-- ============================================

BEGIN;

-- Drop ML-era tables (no data worth keeping)
DROP TABLE IF EXISTS model_metrics CASCADE;
DROP TABLE IF EXISTS predictions CASCADE;
DROP TABLE IF EXISTS features CASCADE;
DROP TABLE IF EXISTS models CASCADE;

-- ============================================
-- New raw ingestion tables
-- ============================================

CREATE TABLE IF NOT EXISTS raw_season_standings (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL,
    team_id INTEGER NOT NULL,
    conference VARCHAR(10),
    conference_rank INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_pct DECIMAL(5,3),
    games_back DECIMAL(5,1),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, team_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_standings_season ON raw_season_standings(season);

CREATE TABLE IF NOT EXISTS raw_player_season_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    season VARCHAR(10) NOT NULL,
    team_id INTEGER,
    games_played INTEGER,
    points_per_game DECIMAL(5,1),
    rebounds_per_game DECIMAL(5,1),
    assists_per_game DECIMAL(5,1),
    steals_per_game DECIMAL(4,2),
    blocks_per_game DECIMAL(4,2),
    fg_pct DECIMAL(5,3),
    three_pt_pct DECIMAL(5,3),
    ft_pct DECIMAL(5,3),
    minutes_per_game DECIMAL(5,1),
    player_efficiency_rating DECIMAL(5,2),
    win_shares DECIMAL(6,2),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_raw_player_season_stats_player ON raw_player_season_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_raw_player_season_stats_season ON raw_player_season_stats(season);

CREATE TABLE IF NOT EXISTS raw_shot_zone_splits (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    season VARCHAR(10) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    fga INTEGER,
    fgm INTEGER,
    fg_pct DECIMAL(5,3),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season, zone)
);

CREATE INDEX IF NOT EXISTS idx_raw_shot_zones_player ON raw_shot_zone_splits(player_id);
CREATE INDEX IF NOT EXISTS idx_raw_shot_zones_season ON raw_shot_zone_splits(season);

-- ============================================
-- Analytics tables
-- ============================================

CREATE TABLE IF NOT EXISTS player_shooting_zones (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    season VARCHAR(10) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    fga INTEGER,
    fgm INTEGER,
    fg_pct DECIMAL(5,3),
    zone_frequency DECIMAL(5,3),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season, zone)
);

CREATE INDEX IF NOT EXISTS idx_shooting_zones_player ON player_shooting_zones(player_id);
CREATE INDEX IF NOT EXISTS idx_shooting_zones_season ON player_shooting_zones(season);

CREATE TABLE IF NOT EXISTS mvp_winners (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL UNIQUE,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    team_id INTEGER REFERENCES teams(team_id),
    team_abbr VARCHAR(10)
);

INSERT INTO mvp_winners (season, player_id, player_name, team_id, team_abbr) VALUES
('2015-16', 201939,  'Stephen Curry',            1610612744, 'GSW'),
('2016-17', 201566,  'Russell Westbrook',         1610612760, 'OKC'),
('2017-18', 201935,  'James Harden',              1610612745, 'HOU'),
('2018-19', 203507,  'Giannis Antetokounmpo',     1610612749, 'MIL'),
('2019-20', 203507,  'Giannis Antetokounmpo',     1610612749, 'MIL'),
('2020-21', 203999,  'Nikola Jokic',              1610612743, 'DEN'),
('2021-22', 203999,  'Nikola Jokic',              1610612743, 'DEN'),
('2022-23', 203954,  'Joel Embiid',               1610612755, 'PHI'),
('2023-24', 203999,  'Nikola Jokic',              1610612743, 'DEN'),
('2024-25', 1628983, 'Shai Gilgeous-Alexander',   1610612760, 'OKC')
ON CONFLICT (season) DO NOTHING;

CREATE TABLE IF NOT EXISTS mvp_season_profiles (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    points_per_game DECIMAL(5,1),
    rebounds_per_game DECIMAL(5,1),
    assists_per_game DECIMAL(5,1),
    fg_pct DECIMAL(5,3),
    three_pt_pct DECIMAL(5,3),
    player_efficiency_rating DECIMAL(5,2),
    win_shares DECIMAL(6,2),
    team_wins INTEGER,
    team_seed INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_mvp_profiles_season ON mvp_season_profiles(season);

CREATE TABLE IF NOT EXISTS playoff_upsets (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL,
    round VARCHAR(30) NOT NULL,
    higher_seed_team_id INTEGER REFERENCES teams(team_id),
    higher_seed_rank INTEGER,
    lower_seed_team_id INTEGER REFERENCES teams(team_id),
    lower_seed_rank INTEGER,
    series_result VARCHAR(10),
    upset_margin INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, round, higher_seed_team_id, lower_seed_team_id)
);

CREATE INDEX IF NOT EXISTS idx_playoff_upsets_season ON playoff_upsets(season);

CREATE TABLE IF NOT EXISTS personal_stats (
    id SERIAL PRIMARY KEY,
    stat_name VARCHAR(100) NOT NULL UNIQUE,
    stat_value TEXT,
    notes TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
