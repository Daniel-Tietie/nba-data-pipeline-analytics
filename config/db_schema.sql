-- ============================================
-- NBA Analytics Platform - Database Schema
-- Fresh install only. DO NOT run against an
-- existing database; use migrations instead.
-- ============================================

-- Drop all tables in reverse dependency order
DROP TABLE IF EXISTS personal_stats CASCADE;
DROP TABLE IF EXISTS playoff_upsets CASCADE;
DROP TABLE IF EXISTS mvp_season_profiles CASCADE;
DROP TABLE IF EXISTS mvp_winners CASCADE;
DROP TABLE IF EXISTS player_shooting_zones CASCADE;
DROP TABLE IF EXISTS raw_shot_zone_splits CASCADE;
DROP TABLE IF EXISTS raw_playoff_games CASCADE;
DROP TABLE IF EXISTS raw_league_season_averages CASCADE;
DROP TABLE IF EXISTS raw_player_season_stats CASCADE;
DROP TABLE IF EXISTS raw_season_standings CASCADE;
DROP TABLE IF EXISTS data_quality_checks CASCADE;
DROP TABLE IF EXISTS dag_runs CASCADE;
DROP TABLE IF EXISTS team_stats CASCADE;
DROP TABLE IF EXISTS games CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS raw_player_stats CASCADE;
DROP TABLE IF EXISTS raw_team_stats CASCADE;
DROP TABLE IF EXISTS raw_games CASCADE;

-- ============================================
-- RAW LAYER - Direct API Data
-- ============================================

CREATE TABLE raw_games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) UNIQUE NOT NULL,
    game_date DATE NOT NULL,
    season VARCHAR(10) NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    home_team_score INTEGER,
    away_team_score INTEGER,
    game_status VARCHAR(20),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_games_date ON raw_games(game_date);
CREATE INDEX idx_raw_games_season ON raw_games(season);

CREATE TABLE raw_team_stats (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL,
    game_id VARCHAR(50),
    stat_date DATE NOT NULL,
    wins INTEGER,
    losses INTEGER,
    win_pct DECIMAL(5,3),
    points_per_game DECIMAL(5,1),
    opp_points_per_game DECIMAL(5,1),
    field_goal_pct DECIMAL(5,3),
    three_point_pct DECIMAL(5,3),
    free_throw_pct DECIMAL(5,3),
    rebounds_per_game DECIMAL(5,1),
    assists_per_game DECIMAL(5,1),
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_team_stats_team ON raw_team_stats(team_id);
CREATE INDEX idx_raw_team_stats_date ON raw_team_stats(stat_date);

CREATE TABLE raw_player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id VARCHAR(50),
    team_id INTEGER NOT NULL,
    stat_date DATE NOT NULL,
    points INTEGER,
    rebounds INTEGER,
    assists INTEGER,
    minutes_played INTEGER,
    raw_data JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_player_stats_player ON raw_player_stats(player_id);
CREATE INDEX idx_raw_player_stats_game ON raw_player_stats(game_id);

CREATE TABLE raw_season_standings (
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

CREATE INDEX idx_raw_standings_season ON raw_season_standings(season);

CREATE TABLE raw_player_season_stats (
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

CREATE INDEX idx_raw_player_season_stats_player ON raw_player_season_stats(player_id);
CREATE INDEX idx_raw_player_season_stats_season ON raw_player_season_stats(season);

CREATE TABLE raw_league_season_averages (
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

CREATE TABLE raw_shot_zone_splits (
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

CREATE INDEX idx_raw_shot_zones_player ON raw_shot_zone_splits(player_id);
CREATE INDEX idx_raw_shot_zones_season ON raw_shot_zone_splits(season);

CREATE TABLE raw_playoff_games (
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

CREATE INDEX idx_raw_playoff_games_season ON raw_playoff_games(season);
CREATE INDEX idx_raw_playoff_games_team ON raw_playoff_games(team_id);

-- ============================================
-- PROCESSED LAYER - Clean Data
-- ============================================

CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    team_abbr VARCHAR(10) NOT NULL,
    city VARCHAR(100),
    conference VARCHAR(10),
    division VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE games (
    game_id VARCHAR(50) PRIMARY KEY,
    game_date DATE NOT NULL,
    season VARCHAR(10) NOT NULL,
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    home_score INTEGER,
    away_score INTEGER,
    winner_id INTEGER REFERENCES teams(team_id),
    point_differential INTEGER,
    total_score INTEGER,
    is_playoff BOOLEAN DEFAULT FALSE,
    game_status VARCHAR(20),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_games_date ON games(game_date);
CREATE INDEX idx_games_season ON games(season);
CREATE INDEX idx_games_home_team ON games(home_team_id);
CREATE INDEX idx_games_away_team ON games(away_team_id);

CREATE TABLE team_stats (
    id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id),
    stat_date DATE NOT NULL,
    season VARCHAR(10) NOT NULL,
    games_played INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_pct DECIMAL(5,3),
    avg_points DECIMAL(5,1),
    avg_opp_points DECIMAL(5,1),
    point_diff DECIMAL(5,1),
    home_record VARCHAR(10),
    away_record VARCHAR(10),
    last_5_record VARCHAR(10),
    fg_pct DECIMAL(5,3),
    three_pt_pct DECIMAL(5,3),
    ft_pct DECIMAL(5,3),
    rebounds_per_game DECIMAL(5,1),
    assists_per_game DECIMAL(5,1),
    turnovers_per_game DECIMAL(5,1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, stat_date)
);

CREATE INDEX idx_team_stats_team ON team_stats(team_id);
CREATE INDEX idx_team_stats_date ON team_stats(stat_date);

-- ============================================
-- ANALYTICS LAYER
-- ============================================

CREATE TABLE player_shooting_zones (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    season VARCHAR(10) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    fga INTEGER,
    fgm INTEGER,
    fg_pct DECIMAL(5,3),
    zone_frequency DECIMAL(5,3),
    league_fg_pct DECIMAL(5,3),
    league_attempt_share DECIMAL(5,3),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season, zone)
);

CREATE INDEX idx_shooting_zones_player ON player_shooting_zones(player_id);
CREATE INDEX idx_shooting_zones_season ON player_shooting_zones(season);

CREATE TABLE mvp_winners (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL UNIQUE,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    team_id INTEGER REFERENCES teams(team_id),
    team_abbr VARCHAR(10)
);

CREATE TABLE mvp_season_profiles (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    points_per_game DECIMAL(5,1),
    rebounds_per_game DECIMAL(5,1),
    assists_per_game DECIMAL(5,1),
    fg_pct DECIMAL(5,3),
    three_pt_pct DECIMAL(5,3),
    ts_pct DECIMAL(5,3),
    player_efficiency_rating DECIMAL(5,2),
    win_shares DECIMAL(6,2),
    team_wins INTEGER,
    team_seed INTEGER,
    team_win_pct DECIMAL(5,3),
    league_avg_pts DECIMAL(5,1),
    league_avg_reb DECIMAL(5,1),
    league_avg_ast DECIMAL(5,1),
    league_avg_ts_pct DECIMAL(5,3),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, player_id)
);

CREATE INDEX idx_mvp_profiles_season ON mvp_season_profiles(season);

CREATE TABLE playoff_upsets (
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

CREATE INDEX idx_playoff_upsets_season ON playoff_upsets(season);

CREATE TABLE personal_stats (
    id SERIAL PRIMARY KEY,
    stat_name VARCHAR(100) NOT NULL UNIQUE,
    stat_value TEXT,
    notes TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- MONITORING LAYER - Pipeline Health
-- ============================================

CREATE TABLE dag_runs (
    run_id SERIAL PRIMARY KEY,
    dag_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    records_processed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dag_runs_name ON dag_runs(dag_name);
CREATE INDEX idx_dag_runs_date ON dag_runs(execution_date);

CREATE TABLE data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    check_date TIMESTAMP NOT NULL,
    passed BOOLEAN NOT NULL,
    records_checked INTEGER,
    records_failed INTEGER,
    failure_rate DECIMAL(5,3),
    check_details JSONB,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_checks_table ON data_quality_checks(table_name);
CREATE INDEX idx_quality_checks_date ON data_quality_checks(check_date);

-- ============================================
-- SEED DATA - NBA Teams
-- ============================================

INSERT INTO teams (team_id, team_name, team_abbr, city, conference, division) VALUES
(1610612737, 'Atlanta Hawks', 'ATL', 'Atlanta', 'East', 'Southeast'),
(1610612738, 'Boston Celtics', 'BOS', 'Boston', 'East', 'Atlantic'),
(1610612751, 'Brooklyn Nets', 'BKN', 'Brooklyn', 'East', 'Atlantic'),
(1610612766, 'Charlotte Hornets', 'CHA', 'Charlotte', 'East', 'Southeast'),
(1610612741, 'Chicago Bulls', 'CHI', 'Chicago', 'East', 'Central'),
(1610612739, 'Cleveland Cavaliers', 'CLE', 'Cleveland', 'East', 'Central'),
(1610612742, 'Dallas Mavericks', 'DAL', 'Dallas', 'West', 'Southwest'),
(1610612743, 'Denver Nuggets', 'DEN', 'Denver', 'West', 'Northwest'),
(1610612765, 'Detroit Pistons', 'DET', 'Detroit', 'East', 'Central'),
(1610612744, 'Golden State Warriors', 'GSW', 'Golden State', 'West', 'Pacific'),
(1610612745, 'Houston Rockets', 'HOU', 'Houston', 'West', 'Southwest'),
(1610612754, 'Indiana Pacers', 'IND', 'Indiana', 'East', 'Central'),
(1610612746, 'LA Clippers', 'LAC', 'Los Angeles', 'West', 'Pacific'),
(1610612747, 'Los Angeles Lakers', 'LAL', 'Los Angeles', 'West', 'Pacific'),
(1610612763, 'Memphis Grizzlies', 'MEM', 'Memphis', 'West', 'Southwest'),
(1610612748, 'Miami Heat', 'MIA', 'Miami', 'East', 'Southeast'),
(1610612749, 'Milwaukee Bucks', 'MIL', 'Milwaukee', 'East', 'Central'),
(1610612750, 'Minnesota Timberwolves', 'MIN', 'Minnesota', 'West', 'Northwest'),
(1610612740, 'New Orleans Pelicans', 'NOP', 'New Orleans', 'West', 'Southwest'),
(1610612752, 'New York Knicks', 'NYK', 'New York', 'East', 'Atlantic'),
(1610612760, 'Oklahoma City Thunder', 'OKC', 'Oklahoma City', 'West', 'Northwest'),
(1610612753, 'Orlando Magic', 'ORL', 'Orlando', 'East', 'Southeast'),
(1610612755, 'Philadelphia 76ers', 'PHI', 'Philadelphia', 'East', 'Atlantic'),
(1610612756, 'Phoenix Suns', 'PHX', 'Phoenix', 'West', 'Pacific'),
(1610612757, 'Portland Trail Blazers', 'POR', 'Portland', 'West', 'Northwest'),
(1610612758, 'Sacramento Kings', 'SAC', 'Sacramento', 'West', 'Pacific'),
(1610612759, 'San Antonio Spurs', 'SAS', 'San Antonio', 'West', 'Southwest'),
(1610612761, 'Toronto Raptors', 'TOR', 'Toronto', 'East', 'Atlantic'),
(1610612762, 'Utah Jazz', 'UTA', 'Utah', 'West', 'Northwest'),
(1610612764, 'Washington Wizards', 'WAS', 'Washington', 'East', 'Southeast');

-- ============================================
-- SEED DATA - MVP Winners (last 10 seasons)
-- ============================================

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
('2024-25', 1628983, 'Shai Gilgeous-Alexander',   1610612760, 'OKC');
