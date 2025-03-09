-- In Amazon Redshift, foreign keys exist but are not enforced 
-- Even if we define FKs in our schema, Redshift does not enforce referential integrity constraints.

-- Create Dim_Seasons table
CREATE TABLE Dim_Seasons (
    Season_ID INT PRIMARY KEY,
    Season_Year INT
);

-- Create Dim_Countries table
CREATE TABLE Dim_Countries (
    Country_ID INT PRIMARY KEY,
    Country_Name VARCHAR(255)
);

-- Create Dim_Leagues table
CREATE TABLE Dim_Leagues (
    League_ID INT PRIMARY KEY,
    League_Name VARCHAR(255),
    Country_ID INT,
    League_Logo VARCHAR(255),
    FOREIGN KEY (Country_ID) REFERENCES Dim_Countries (Country_ID)
);

-- Create Dim_Venues table
CREATE TABLE Dim_Venues (
    Venue_ID INT PRIMARY KEY,
    Venue_Name VARCHAR(255),
    City VARCHAR(255),
    Country_ID INT,
    Capacity INT,
    FOREIGN KEY (Country_ID) REFERENCES Dim_Countries (Country_ID)
);

-- Create Dim_Teams table
CREATE TABLE Dim_Teams (
    Team_ID INT PRIMARY KEY,
    Team_Name VARCHAR(255),
    League_ID INT,
    Home_Venue_ID INT,
    Team_Logo VARCHAR(255),
    FOREIGN KEY (League_ID) REFERENCES Dim_Leagues (League_ID),
    FOREIGN KEY (Home_Venue_ID) REFERENCES Dim_Venues (Venue_ID)
);

-- Create Dim_Players table
CREATE TABLE Dim_Players (
    Player_ID INT PRIMARY KEY,
    Player_Name VARCHAR(255),
    Team_ID INT,
    FOREIGN KEY (Team_ID) REFERENCES Dim_Teams (Team_ID)
);

-- Create Dim_Fixtures table
CREATE TABLE Dim_Fixtures (
    Fixture_ID INT PRIMARY KEY,
    League_ID INT,
    Season_ID INT,
    Home_Team_ID INT,
    Away_Team_ID INT,
    Venue_ID INT,
    Match_Date TIMESTAMP,
    Match_Time TIMESTAMP,
    FOREIGN KEY (League_ID) REFERENCES Dim_Leagues (League_ID),
    FOREIGN KEY (Season_ID) REFERENCES Dim_Seasons (Season_ID),
    FOREIGN KEY (Home_Team_ID) REFERENCES Dim_Teams (Team_ID),
    FOREIGN KEY (Away_Team_ID) REFERENCES Dim_Teams (Team_ID),
    FOREIGN KEY (Venue_ID) REFERENCES Dim_Venues (Venue_ID)
);

-- Create Fact_Player_Statistics table
CREATE TABLE Fact_Player_Statistics (
    Player_Stat_ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    Fixture_ID INT,
    Team_ID INT,
    Player_ID INT,
    Minutes_Played INT,
    Position VARCHAR(255),
    Rating FLOAT,
    Substitute BOOLEAN,
    Goals_Scored INT,
    Goals_Conceded INT,
    Assists INT,
    Saves INT,
    Passes INT,
    Passes_Key INT,
    Pass_Accuracy FLOAT,
    Yellow_Cards INT,
    Red_Cards INT,
    Shots INT,
    Shots_On_Target INT,
    Tackles INT,
    Blocks INT,
    Interceptions INT,
    Duels INT,
    Duels_Won INT,
    Dribbles_Attempted INT,
    Dribbles_Success INT,
    Dribbled_Past INT,
    Fouls_Committed INT,
    Fouls_Drawn INT,
    Penalty_Scored INT,
    Penalty_Missed INT,
    Penalty_Saved INT,
    Penalty_Won INT,
    Penalty_Committed INT,
    Offside INT,
    FOREIGN KEY (Fixture_ID) REFERENCES Dim_Fixtures (Fixture_ID),
    FOREIGN KEY (Team_ID) REFERENCES Dim_Teams (Team_ID),
    FOREIGN KEY (Player_ID) REFERENCES Dim_Players (Player_ID)
);

-- Create Fact_Team_Statistics table
CREATE TABLE Fact_Team_Statistics (
    Team_Stat_ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    Fixture_ID INT,
    Team_ID INT,
    Goals_Scored INT,
    Goals_Conceded INT,
    Winner_Team BOOLEAN,
    Shots_On_Target INT,
    Shots_Off_Target INT,
    Total_Shots INT,
    Blocked_Shots INT,
    Shots_Inside_Box INT,
    Shots_Outside_Box INT,
    Fouls INT,
    Corner_Kicks INT,
    Offsides INT,
    Possession_Percentage FLOAT,
    Yellow_Cards INT,
    Red_Cards INT,
    Goalkeeper_Saves INT,
    Total_Passes INT,
    Passes_Accurate INT,
    Passes_Percentage FLOAT,
    Expected_Goals FLOAT,
    FOREIGN KEY (Fixture_ID) REFERENCES Dim_Fixtures (Fixture_ID),
    FOREIGN KEY (Team_ID) REFERENCES Dim_Teams (Team_ID)
);