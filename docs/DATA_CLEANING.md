# Data Cleaning Documentation

## Overview

This document tracks data quality issues discovered during ETL and how they were resolved.

---

## Issue 1: Invalid Team IDs from NBA API

**Discovered:** October 27, 2025  
**Severity:** Low (0.4% of data)

### Problem Description

The NBA Stats API returned 8 invalid team IDs that do not correspond to actual NBA teams:
- Team IDs: 1610616833, 1610616834, 1610616839, 1610616840, 1610616847, 1610616848, 1610616849, 1610616850
- These IDs were present in 15 games across the 2021-2024 seasons
- Likely caused by: All-Star games, exhibition games, or API data errors

### Investigation
```sql
-- Query to identify unknown teams
SELECT DISTINCT home_team_id FROM raw_games 
WHERE home_team_id NOT IN (SELECT team_id FROM teams WHERE is_active = TRUE)
UNION
SELECT DISTINCT away_team_id FROM raw_games 
WHERE away_team_id NOT IN (SELECT team_id FROM teams WHERE is_active = TRUE);
```

**Findings:**
- 15 games involved these invalid team IDs (0.4% of 3,706 total games)
- Each unknown team appeared in approximately 2-3 games
- Games occurred on dates with legitimate NBA games (not isolated incidents)

### Resolution

**Action Taken:**
1. Deleted 15 games involving invalid team IDs from both `raw_games` and `games` tables
2. Removed 8 unknown team records from `teams` table
3. Re-calculated team statistics with clean data

**SQL Commands:**
```sql
DELETE FROM team_stats WHERE team_id >= 1610616833 AND team_id <= 1610616850;
DELETE FROM games WHERE home_team_id >= 1610616833 AND home_team_id <= 1610616850
   OR away_team_id >= 1610616833 AND away_team_id <= 1610616850;
DELETE FROM raw_games WHERE home_team_id >= 1610616833 AND home_team_id <= 1610616850
   OR away_team_id >= 1610616833 AND away_team_id <= 1610616850;
DELETE FROM teams WHERE team_id >= 1610616833 AND team_id <= 1610616850;
```

**Impact:**
- Before: 3,706 games, 38 teams
- After: 3,691 games, 30 teams
- Data loss: 15 games (0.4%)
- Quality improvement: 100% valid NBA team data

### Prevention

To prevent similar issues in future data ingestion:

1. **Validation Layer:** Add team ID validation in `src/ingestion/nba_api_client.py`
```python
   VALID_TEAM_ID_RANGE = (1610612737, 1610612766)
   
   def validate_team_id(team_id: int) -> bool:
       return VALID_TEAM_ID_RANGE[0] <= team_id <= VALID_TEAM_ID_RANGE[1]
```

2. **Pre-ingestion Filter:** Filter out invalid games before database insertion

3. **Data Quality Checks:** Add automated checks to flag unknown team IDs

---

## Issue 2: Duplicate Team Stats on Same Date

**Discovered:** October 27, 2025  
**Severity:** Medium (caused ETL failures)

### Problem Description

Team statistics calculation was generating multiple records for the same team on the same date, violating the `UNIQUE` constraint on `(team_id, stat_date)`.

**Root Cause:** Invalid team IDs (Issue 1) appeared in multiple games on the same date, causing the window function in the stats calculation query to generate duplicate aggregations.

### Resolution

**Primary Fix:** Resolved by cleaning invalid team data (Issue 1)

**Secondary Fix:** Added `DISTINCT ON (team_id, stat_date)` clause in team stats query as a safeguard:
```sql
SELECT DISTINCT ON (team_id, stat_date)
    team_id, stat_date, season, ...
FROM team_game_stats
ORDER BY team_id, stat_date, games_played DESC
```

This ensures only one stat record per team per date, keeping the record with the highest `games_played`.

**Impact:**
- ETL pipeline now runs successfully
- No duplicate stat records
- Stats represent end-of-day cumulative performance

---

## Data Quality Metrics (Post-Cleaning)

| Metric | Value | Status |
|--------|-------|--------|
| Total Games | 3,691 | ✓ Clean |
| Total Teams | 30 | ✓ Complete |
| Date Range | Oct 2021 - Apr 2024 | ✓ Valid |
| Missing Scores | 0 | ✓ Complete |
| Invalid Team IDs | 0 | ✓ Clean |
| Duplicate Stats | 0 | ✓ Clean |
| Data Completeness | 99.6% | ✓ Excellent |

---

## Future Improvements

1. **Add Pre-Ingestion Validation:**
   - Team ID range checks
   - Game date validation
   - Score reasonableness checks

2. **Automated Data Quality Checks:**
   - Build `src/etl/data_quality.py` module
   - Run validation after each ETL step
   - Generate data quality report

3. **Alerting:**
   - Log warnings for suspicious data
   - Notifications for data quality issues in production

---

*Last Updated: October 27, 2025*