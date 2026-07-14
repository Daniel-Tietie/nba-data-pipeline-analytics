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

## Issue 3: Incorrect Player/Team IDs in mvp_winners Seed Data

**Discovered:** July 13, 2026
**Severity:** High (would have silently pulled the wrong player's stats for 6 of 10 seasons)

### Problem Description

The hand-written `mvp_winners` seed data had wrong `player_id` values for six
of the ten seasons. Some pointed at a real but incorrect player (2016-17,
2017-18, 2018-19, 2019-20 all resolved to the wrong person), and 2022-23 named
the wrong winner outright (Jokic instead of Embiid). 2023-24 and 2024-25 were
both mapped to Shai Gilgeous-Alexander's 2024-25 ID, one season off.

### Investigation

Cross-checked each seed row's `player_name` against the live
`leaguedashplayerstats` response for that season, matching on
accent-normalized name (`unicodedata.normalize("NFKD", ...)`) rather than
trusting the hardcoded `player_id`.

### Resolution

**Action Taken:**
1. Resolved the correct `player_id`, `team_id`, and `team_abbr` for all 10
   seasons directly from the stats API.
2. Reseeded `mvp_winners` and cleared/repopulated `raw_player_season_stats`
   with the corrected IDs.
3. Updated `config/db_schema.sql` and `config/migrations/002_pivot_to_analytics.sql`
   to match, so a fresh install seeds correctly instead of reproducing the bug.

**Impact:**
- Before: 6 of 10 MVP seasons pointed at the wrong player's stats.
- After: all 10 verified against the live API by name match.

### Prevention

Any future hardcoded ID seed data for this project should be resolved from
the API by name at write time, not typed from memory.

---

## Issue 4: leaguestandingsv3 conference_rank Doesn't Reflect Play-In Results

**Discovered:** July 14, 2026
**Severity:** Medium (wrong seed number shown for one team per conference in play-in seasons; did not affect win/loss results)

### Problem Description

`raw_season_standings.conference_rank` (sourced from the API's `PlayoffRank`
field) is the team's rank by regular-season record. For seasons with a
play-in tournament (2020-21 onward), that's not always the same as the
seed the team actually entered the playoff bracket with: the 7-vs-8 play-in
game winner takes the "7" bracket slot regardless of which team had the
better regular-season record.

**Example:** In 2022-23, Miami finished the regular season 44-38 (better
than Atlanta's 41-41) and is stored with `conference_rank = 7`. But Atlanta
beat Miami in the 7-vs-8 play-in game, so Atlanta took the 7 seed and played
Boston (the 2-seed) in round 1, while Miami dropped to the 8 seed and played
Milwaukee (the 1-seed) -- the team that then upset Milwaukee in 5 games.

### Investigation

Cross-checked `playoff_upsets`' round-1 pairings against `raw_playoff_games`
directly: queried Boston's (2-seed) round-1 opponent and confirmed it was
Atlanta, not Miami, which only makes sense if Atlanta held the 7 bracket
slot and Miami the 8, opposite of their `conference_rank` values.

### Resolution

`build_playoff_upsets.py` doesn't read the opponent's seed from
`raw_season_standings` at all. Round 1 in every one of these seasons is a
standard 1-vs-8 bracket by construction, so the 1-seed's round-1 opponent's
seed is hardcoded to 8 rather than looked up. This sidesteps the mislabeled
field entirely rather than trying to correct it.

**Impact:** Only affects the seed *number* displayed for the round-1
opponent; the win/loss tally and upset boolean were always derived directly
from game results and were never wrong.

### Prevention

Don't trust a "rank" field name at face value when a mid-season tournament
can reorder it. Verify against ground-truth game data (who actually played
whom) before using a standings field for bracket logic.

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

*Last Updated: July 14, 2026*