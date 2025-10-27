-- ============================================
-- NBA Data Pipeline - Database Schema
-- ============================================

-- Drop existing tables (for clean setup)
DROP TABLE IF EXISTS data_quality_checks CASCADE;
DROP TABLE IF EXISTS dag_runs CASCADE;
DROP TABLE IF EXISTS model_metrics CASCADE;
DROP TABLE IF EXISTS predictions CASCADE;
DROP TABLE IF EXISTS features CASCADE;
DROP TABLE IF EXISTS models CASCADE;
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
-- ML LAYER - Features and Predictions
-- ============================================

CREATE TABLE models (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50),
    hyperparameters JSONB,
    training_date TIMESTAMP NOT NULL,
    training_samples INTEGER,
    is_active BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(model_name, model_version)
);

CREATE TABLE features (
    feature_id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) REFERENCES games(game_id),
    home_team_win_pct DECIMAL(5,3),
    away_team_win_pct DECIMAL(5,3),
    home_team_last_5_wins INTEGER,
    away_team_last_5_wins INTEGER,
    h2h_home_wins INTEGER,
    h2h_away_wins INTEGER,
    h2h_total_games INTEGER,
    home_team_days_rest INTEGER,
    away_team_days_rest INTEGER,
    home_team_avg_points DECIMAL(5,1),
    away_team_avg_points DECIMAL(5,1),
    home_team_avg_opp_points DECIMAL(5,1),
    away_team_avg_opp_points DECIMAL(5,1),
    home_team_point_diff DECIMAL(5,1),
    away_team_point_diff DECIMAL(5,1),
    home_team_fg_pct DECIMAL(5,3),
    away_team_fg_pct DECIMAL(5,3),
    home_team_three_pt_pct DECIMAL(5,3),
    away_team_three_pt_pct DECIMAL(5,3),
    is_back_to_back_home BOOLEAN,
    is_back_to_back_away BOOLEAN,
    home_team_won BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_features_game ON features(game_id);

CREATE TABLE predictions (
    prediction_id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) REFERENCES games(game_id),
    model_id INTEGER REFERENCES models(model_id),
    predicted_winner_id INTEGER REFERENCES teams(team_id),
    predicted_winner_name VARCHAR(100),
    win_probability DECIMAL(5,3),
    predicted_margin DECIMAL(5,1),
    actual_winner_id INTEGER REFERENCES teams(team_id),
    was_correct BOOLEAN,
    prediction_date TIMESTAMP NOT NULL,
    game_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, model_id)
);

CREATE INDEX idx_predictions_game ON predictions(game_id);
CREATE INDEX idx_predictions_model ON predictions(model_id);
CREATE INDEX idx_predictions_date ON predictions(game_date);

CREATE TABLE model_metrics (
    metric_id SERIAL PRIMARY KEY,
    model_id INTEGER REFERENCES models(model_id),
    evaluation_date DATE NOT NULL,
    evaluation_period VARCHAR(50),
    total_predictions INTEGER,
    correct_predictions INTEGER,
    accuracy DECIMAL(5,3),
    precision_score DECIMAL(5,3),
    recall_score DECIMAL(5,3),
    f1_score DECIMAL(5,3),
    roc_auc DECIMAL(5,3),
    avg_confidence DECIMAL(5,3),
    calibration_score DECIMAL(5,3),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metrics_model ON model_metrics(model_id);
CREATE INDEX idx_metrics_date ON model_metrics(evaluation_date);

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