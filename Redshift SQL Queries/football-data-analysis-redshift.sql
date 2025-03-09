-- Generating Standings Table
WITH match_results AS (
    SELECT 
        f.Season_ID,
        f.League_ID,
        t.Team_ID,
        t.Team_Name,
        SUM(CASE WHEN ft.Winner_Team = TRUE THEN 3 
                 WHEN ft.Goals_Scored = ft.Goals_Conceded THEN 1 
                 ELSE 0 END) AS League_Points,
        SUM(CASE WHEN ft.Winner_Team = TRUE THEN 1 ELSE 0 END) AS Wins,
        SUM(CASE WHEN ft.Goals_Scored = ft.Goals_Conceded THEN 1 ELSE 0 END) AS Draws,
        SUM(CASE WHEN ft.Winner_Team = FALSE THEN 1 ELSE 0 END) AS Loses,
        SUM(ft.Goals_Scored) AS Goals_For,
        SUM(ft.Goals_Conceded) AS Goals_Against,
        SUM(ft.Goals_Scored) - SUM(ft.Goals_Conceded) AS Goal_Difference,
        SUM(CASE WHEN f.Away_Team_ID = t.Team_ID AND ft.Winner_Team = TRUE THEN 1 ELSE 0 END) AS Away_Wins,
        SUM(CASE WHEN f.Home_Team_ID = t.Team_ID AND ft.Winner_Team = FALSE THEN 1 ELSE 0 END) AS Home_Losses
    FROM Fact_Team_Statistics ft
    JOIN Dim_Fixtures f ON ft.Fixture_ID = f.Fixture_ID
    JOIN Dim_Teams t ON ft.Team_ID = t.Team_ID
    GROUP BY f.Season_ID, f.League_ID, t.Team_ID, t.Team_Name
)
SELECT
    mr.Season_ID,
    mr.League_ID,
    Season_Year,
    League_Name,
    RANK() OVER (ORDER BY League_Points DESC, Goal_Difference DESC, Goals_For DESC) AS League_Position,
    Team_Name AS Team,
    League_Points,
    Wins,
    Draws,
    Loses,
    Goal_Difference,
    Goals_For,
    Goals_Against,
    Away_Wins,
    Home_Losses
FROM match_results mr
LEFT JOIN Dim_Seasons s ON mr.Season_ID = s.Season_ID
LEFT JOIN Dim_Leagues l ON mr.League_ID = l.League_ID
ORDER BY League_Position;

-- Top Goalscorers of the Season
SELECT fp.Player_ID, p.Player_Name, SUM(fp.Goals_Scored) as total_goals
FROM Fact_Player_Statistics fp
JOIN Dim_Players p ON fp.Player_ID=p.Player_ID
GROUP BY fp.Player_ID, p.Player_Name
HAVING SUM(fp.Goals_Scored) is not null
ORDER BY SUM(fp.Goals_Scored) desc
LIMIT 10;

-- Home vs Away Performance Analysis
WITH home_performance AS (
    SELECT 
        f.Season_ID,
        f.League_ID,
        t.Team_ID,
        t.Team_Name,
        SUM(CASE WHEN f.Home_Team_ID = t.Team_ID AND ft.Winner_Team = TRUE THEN 3 
                 WHEN f.Home_Team_ID = t.Team_ID AND ft.Goals_Scored = ft.Goals_Conceded THEN 1 ELSE 0 END) AS Home_Points,
        SUM(CASE WHEN f.Home_Team_ID = t.Team_ID THEN ft.Goals_Scored ELSE 0 END) AS Home_Goals,
        COUNT(*) AS Home_Matches
    FROM Fact_Team_Statistics ft
    JOIN Dim_Fixtures f ON ft.Fixture_ID = f.Fixture_ID
    JOIN Dim_Teams t ON ft.Team_ID = t.Team_ID
    GROUP BY f.Season_ID, f.League_ID, t.Team_ID, t.Team_Name
),
away_performance AS (
    SELECT 
        f.Season_ID,
        f.League_ID,
        t.Team_ID,
        SUM(CASE WHEN f.Away_Team_ID = t.Team_ID AND ft.Winner_Team = TRUE THEN 3 
                 WHEN f.Away_Team_ID = t.Team_ID AND ft.Goals_Scored = ft.Goals_Conceded THEN 1 ELSE 0 END) AS Away_Points,
        SUM(CASE WHEN f.Away_Team_ID = t.Team_ID THEN ft.Goals_Scored ELSE 0 END) AS Away_Goals,
        COUNT(*) AS Away_Matches
    FROM Fact_Team_Statistics ft
    JOIN Dim_Fixtures f ON ft.Fixture_ID = f.Fixture_ID
    JOIN Dim_Teams t ON ft.Team_ID = t.Team_ID
    GROUP BY f.Season_ID, f.League_ID, t.Team_ID
)
SELECT 
    h.Season_ID,
    h.League_ID,
    t.Team_Name,
    Home_Points,
    Away_Points,
    Home_Goals,
    Away_Goals,
    ROUND((Home_Points * 1.0 / NULLIF(Home_Matches, 0)), 2) AS Avg_Home_Points,
    ROUND((Away_Points * 1.0 / NULLIF(Away_Matches, 0)), 2) AS Avg_Away_Points
FROM home_performance h
JOIN away_performance a ON h.Team_ID = a.Team_ID
JOIN Dim_Teams t ON h.Team_ID = t.Team_ID
ORDER BY (Home_Points + Away_Points) DESC;

-- Most Consistent Players (Min 10 Matches)
SELECT 
    p.Player_Name,
    p.Team_ID,
    t.Team_Name,
    COUNT(ps.Fixture_ID) AS Matches_Played,
    ROUND(AVG(ps.Rating), 2) AS Avg_Rating
FROM Fact_Player_Statistics ps
JOIN Dim_Players p ON ps.Player_ID = p.Player_ID
JOIN Dim_Teams t ON p.Team_ID = t.Team_ID
WHERE ps.Minutes_Played >= 60  -- Played most of the match
GROUP BY p.Player_ID, p.Player_Name, p.Team_ID, t.Team_Name
HAVING COUNT(ps.Fixture_ID) >= 10
ORDER BY Avg_Rating DESC
LIMIT 10;

-- Most Impactful Players (Goals + Assists per Match)
SELECT 
    p.Player_Name,
    t.Team_Name,
    COUNT(ps.Fixture_ID) AS Matches_Played,
    SUM(ps.Goals_Scored) AS Total_Goals,
    SUM(ps.Assists) AS Total_Assists,
    SUM(ps.Goals_Scored + ps.Assists) AS Goal_Contributions,
    ROUND(AVG(ps.Goals_Scored + ps.Assists), 2) AS Avg_Goal_Contributions
FROM Fact_Player_Statistics ps
JOIN Dim_Players p ON ps.Player_ID = p.Player_ID
JOIN Dim_Teams t ON p.Team_ID = t.Team_ID
GROUP BY p.Player_ID, p.Player_Name, t.Team_Name
HAVING COUNT(ps.Fixture_ID) >= 10  -- Played at least 10 matches
ORDER BY Goal_Contributions DESC
LIMIT 10;

-- Best Goalkeepers (Min. 10 Matches)
WITH goalkeeper_stats AS (
    SELECT 
        p.Player_Name,
        t.Team_Name,
        COUNT(ps.Fixture_ID) AS Matches_Played,
        SUM(ps.Saves) AS Total_Saves,
        SUM(ps.Goals_Conceded) AS Goals_Allowed,
        ROUND((SUM(ps.Saves) * 100.0 / NULLIF(SUM(ps.Saves + ps.Goals_Conceded), 0)), 2) AS Save_Percentage
    FROM Fact_Player_Statistics ps
    JOIN Dim_Players p ON ps.Player_ID = p.Player_ID
    JOIN Dim_Teams t ON p.Team_ID = t.Team_ID
    WHERE ps.Position = 'G'
    GROUP BY p.Player_ID, p.Player_Name, t.Team_Name
    HAVING COUNT(ps.Fixture_ID) >= 10
)
SELECT * FROM goalkeeper_stats
ORDER BY Save_Percentage DESC
LIMIT 10;

-- Most Aggressive Teams (Fouls + Cards Per Game)
SELECT 
    t.Team_Name,
    COUNT(ft.Fixture_ID) AS Matches_Played,
    SUM(ft.Fouls) AS Total_Fouls,
    SUM(ft.Yellow_Cards) AS Total_Yellows,
    SUM(ft.Red_Cards) AS Total_Reds,
    ROUND(AVG(ft.Fouls), 2) AS Avg_Fouls_Per_Game,
    ROUND(AVG(ft.Yellow_Cards), 2) AS Avg_Yellows_Per_Game,
    ROUND(AVG(ft.Red_Cards), 2) AS Avg_Reds_Per_Game
FROM Fact_Team_Statistics ft
JOIN Dim_Teams t ON ft.Team_ID = t.Team_ID
GROUP BY t.Team_Name
ORDER BY Avg_Fouls_Per_Game DESC
LIMIT 10;