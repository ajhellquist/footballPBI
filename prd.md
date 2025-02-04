Project Goal
Showcase business intelligence skills using college football data in Power BI.
Data Collection (via CollegeFootballData.com API)
Scope: Historical game results & team stats (2000–present)
API: Uses a provided API key
One-time ETL execution
ETL Process (Python)
Automated ETL script (manual trigger via IDE)
Error handling: Terminal success/failure messages (no logs)
Cloud-hosted MongoDB
Collections:
games
records
player_stats
team_stats
Database Optimization
Indexes for faster queries on { team: 1, year: 1 }
Data stored raw (aggregations in Power BI)
Power BI Setup
Native MongoDB connector
Import Mode (data loaded into Power BI)
Planned visuals:
Team performance trends
Team comparisons (offense/defense)
Player tracking over time
Advanced analytics (optional)
