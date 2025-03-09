import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id, when, to_timestamp, date_format, explode, regexp_replace

# Read Raw Data from AWS Glue Catalog
fixtures_df = glueContext.create_dynamic_frame.from_catalog(
    database="football_db", table_name="fixtures"
).toDF()
teams_df = glueContext.create_dynamic_frame.from_catalog(
    database="football_db", table_name="teams"
).toDF()
team_stats_df = glueContext.create_dynamic_frame.from_catalog(
    database="football_db", table_name="team_stats"
).toDF()
players_df = glueContext.create_dynamic_frame.from_catalog(
    database="football_db", table_name="player_stats"
).toDF()

# Explode the response array to get individual match records
fixtures_exploded = fixtures_df.selectExpr("explode(response) as match_data")
# Manually populate Dim_Seasons
dim_seasons_data = [(2021, 1), (2022, 2), (2023, 3), (2024, 4), (2025, 5)]
dim_seasons_df = spark.createDataFrame(dim_seasons_data, ["Season_Year", "Season_ID"])
dim_seasons_df.show()
seasons_dynamic_frame = DynamicFrame.fromDF(dim_seasons_df, glueContext, "seasons_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_seasons"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = seasons_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Extract Dim_Countries dynamically
dim_countries_data = [("England", 1), ("Spain", 2), ("Germany", 3), ("Italy", 4), ("France", 5)]
dim_countries_df = spark.createDataFrame(dim_countries_data, ["Country_Name", "Country_ID"])
dim_countries_df.show()
countries_dynamic_frame = DynamicFrame.fromDF(dim_countries_df, glueContext, "countries_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_countries"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = countries_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Extract Dim_Leagues dynamically
dim_leagues_df = fixtures_exploded.select(
    col("match_data.league.id").alias("League_ID"),
    col("match_data.league.name").alias("League_Name"),
    col("match_data.league.country").alias("Country_Name"),
    col("match_data.league.logo").alias("League_Logo")
).distinct()

# Assign Country_ID from Dim_Countries
dim_leagues_df = dim_leagues_df.join(dim_countries_df, "Country_Name", "left") \
                             .select("League_ID", "League_Name", "Country_ID", "League_Logo")
dim_leagues_df.show()
leagues_dynamic_frame = DynamicFrame.fromDF(dim_leagues_df, glueContext, "leagues_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_leagues"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = leagues_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Explode response array to extract individual team records
teams_exploded = teams_df.selectExpr("explode(response) as team_data")
# Extract Dim_Venues
dim_venues_df = teams_exploded.select(
    col("team_data.venue.id").alias("Venue_ID"),
    col("team_data.venue.name").alias("Venue_Name"),
    col("team_data.venue.city").alias("City"),
    col("team_data.team.country").alias("Country_Name"),
    col("team_data.venue.capacity").alias("Capacity")
).distinct()

# Assign Country_ID from Dim_Countries
dim_venues_df = dim_venues_df.join(dim_countries_df, "Country_Name", "left") \
                             .select("Venue_ID", "Venue_Name", "City", "Country_ID", "Capacity")
dim_venues_df.show()
venues_dynamic_frame = DynamicFrame.fromDF(dim_venues_df, glueContext, "venues_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_venues"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = venues_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Extract Dim_Teams
dim_teams_df = teams_exploded.select(
    col("team_data.team.id").alias("Team_ID"),
    col("team_data.team.name").alias("Team_Name"),
    col("team_data.team.logo").alias("Team_Logo"),
    col("team_data.team.country").alias("Country_Name"),
    col("team_data.venue.id").alias("Venue_ID")
).distinct()

# Assign League_ID by joining with Dim_Leagues
dim_teams_df = dim_teams_df.join(dim_countries_df, "Country_Name", "left") \
                           .select("Team_ID", "Team_Name", "Venue_ID", "Team_Logo", "Country_ID") \
                           .join(dim_leagues_df, "Country_ID", "left") \
                           .select("Team_ID", "Team_Name", "League_ID", "Venue_ID", "Team_Logo")
dim_teams_df.show()
teams_dynamic_frame = DynamicFrame.fromDF(dim_teams_df, glueContext, "teams_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_teams"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = teams_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Explode the nested structure to extract player information
players_exploded = players_df.selectExpr("explode(response) as response") \
    .selectExpr("response.team.id as Team_ID", "explode(response.players) as player_data") \
    .select(
        col("player_data.player.id").alias("Player_ID"),
        col("player_data.player.name").alias("Player_Name"),
        col("Team_ID")
    )

# Remove duplicates to keep only distinct Player_IDs
dim_players_df = players_exploded.dropDuplicates(["Player_ID"])
dim_players_df.show()
players_dynamic_frame = DynamicFrame.fromDF(dim_players_df, glueContext, "players_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_players"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = players_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Extract Fixture_ID from parameters.fixture
player_stats_df = players_df.withColumn("Fixture_ID", col("parameters.fixture"))

# Explode response array to extract individual fixture records
player_stats_exploded = player_stats_df.selectExpr("Fixture_ID", "explode(response) as match_data")

# Explode players array to get individual player records
player_stats_exploded = player_stats_exploded.select(
    col("Fixture_ID"),
    col("match_data.team.id").alias("Team_ID"),
    col("match_data.team.name").alias("Team_Name"),
    col("match_data.players").alias("Players")
).withColumn("Players", explode(col("Players")))

# Extract Player-Level Statistics
fact_player_stats_df = player_stats_exploded.select(
    col("Fixture_ID").cast("int"),
    col("Team_ID").cast("int"),
    col("Players.player.id").cast("int").alias("Player_ID"),
    when(col("Players.statistics.games.minutes").isNotNull(), col("Players.statistics.games.minutes").getItem(0)).otherwise(lit(None)).cast("int").alias("Minutes_Played"),
    when(col("Players.statistics.games.position").isNotNull(), col("Players.statistics.games.position").getItem(0)).otherwise(lit(None)).alias("Position"),
    when(col("Players.statistics.games.rating").isNotNull(), col("Players.statistics.games.rating").getItem(0)).otherwise(lit(None)).cast("float").alias("Rating"),
    when(col("Players.statistics.games.substitute").isNotNull(), col("Players.statistics.games.substitute").getItem(0)).otherwise(lit(None)).cast("boolean").alias("Substitute"),
    when(col("Players.statistics.goals.total").isNotNull(), col("Players.statistics.goals.total").getItem(0)).otherwise(lit(None)).cast("int").alias("Goals_Scored"),
    when(col("Players.statistics.goals.conceded").isNotNull(), col("Players.statistics.goals.conceded").getItem(0)).otherwise(lit(None)).cast("int").alias("Goals_Conceded"),
    when(col("Players.statistics.goals.assists").isNotNull(), col("Players.statistics.goals.assists").getItem(0)).otherwise(lit(None)).cast("int").alias("Assists"),
    when(col("Players.statistics.goals.saves").isNotNull(), col("Players.statistics.goals.saves").getItem(0)).otherwise(lit(None)).cast("int").alias("Saves"),
    when(col("Players.statistics.passes.total").isNotNull(), col("Players.statistics.passes.total").getItem(0)).otherwise(lit(None)).cast("int").alias("Passes"),
    when(col("Players.statistics.passes.key").isNotNull(), col("Players.statistics.passes.key").getItem(0)).otherwise(lit(None)).cast("int").alias("Passes_Key"),
    when(col("Players.statistics.passes.accuracy").isNotNull(), col("Players.statistics.passes.accuracy").getItem(0)).otherwise(lit(None)).cast("float").alias("Pass_Accuracy"),
    when(col("Players.statistics.cards.yellow").isNotNull(), col("Players.statistics.cards.yellow").getItem(0)).otherwise(lit(None)).cast("int").alias("Yellow_Cards"),
    when(col("Players.statistics.cards.red").isNotNull(), col("Players.statistics.cards.red").getItem(0)).otherwise(lit(None)).cast("int").alias("Red_Cards"),
    when(col("Players.statistics.shots.total").isNotNull(), col("Players.statistics.shots.total").getItem(0)).otherwise(lit(None)).cast("int").alias("Shots"),
    when(col("Players.statistics.shots.on").isNotNull(), col("Players.statistics.shots.on").getItem(0)).otherwise(lit(None)).cast("int").alias("Shots_On_Target"),
    when(col("Players.statistics.tackles.total").isNotNull(), col("Players.statistics.tackles.total").getItem(0)).otherwise(lit(None)).cast("int").alias("Tackles"),
    when(col("Players.statistics.tackles.blocks").isNotNull(), col("Players.statistics.tackles.blocks").getItem(0)).otherwise(lit(None)).cast("int").alias("Blocks"),
    when(col("Players.statistics.tackles.interceptions").isNotNull(), col("Players.statistics.tackles.interceptions").getItem(0)).otherwise(lit(None)).cast("int").alias("Interceptions"),
    when(col("Players.statistics.duels.total").isNotNull(), col("Players.statistics.duels.total").getItem(0)).otherwise(lit(None)).cast("int").alias("Duels"),
    when(col("Players.statistics.duels.won").isNotNull(), col("Players.statistics.duels.won").getItem(0)).otherwise(lit(None)).cast("int").alias("Duels_Won"),
    when(col("Players.statistics.dribbles.attempts").isNotNull(), col("Players.statistics.dribbles.attempts").getItem(0)).otherwise(lit(None)).cast("int").alias("Dribbles_Attempted"),
    when(col("Players.statistics.dribbles.success").isNotNull(), col("Players.statistics.dribbles.success").getItem(0)).otherwise(lit(None)).cast("int").alias("Dribbles_Success"),
    when(col("Players.statistics.dribbles.past").isNotNull(), col("Players.statistics.dribbles.past").getItem(0)).otherwise(lit(None)).cast("int").alias("Dribbled_Past"),
    when(col("Players.statistics.fouls.committed").isNotNull(), col("Players.statistics.fouls.committed").getItem(0)).otherwise(lit(None)).cast("int").alias("Fouls_Committed"),
    when(col("Players.statistics.fouls.drawn").isNotNull(), col("Players.statistics.fouls.drawn").getItem(0)).otherwise(lit(None)).cast("int").alias("Fouls_Drawn"),
    when(col("Players.statistics.penalty.scored").isNotNull(), col("Players.statistics.penalty.scored").getItem(0)).otherwise(lit(None)).cast("int").alias("Penalty_Scored"),
    when(col("Players.statistics.penalty.missed").isNotNull(), col("Players.statistics.penalty.missed").getItem(0)).otherwise(lit(None)).cast("int").alias("Penalty_Missed"),
    when(col("Players.statistics.penalty.saved").isNotNull(), col("Players.statistics.penalty.saved").getItem(0)).otherwise(lit(None)).cast("int").alias("Penalty_Saved"),
    when(col("Players.statistics.penalty.won").isNotNull(), col("Players.statistics.penalty.won").getItem(0)).otherwise(lit(None)).cast("int").alias("Penalty_Won"),
    when(col("Players.statistics.penalty.commited").isNotNull(), col("Players.statistics.penalty.commited").getItem(0)).otherwise(lit(None)).cast("int").alias("Penalty_Commited"),
    when(col("Players.statistics.offsides").isNotNull(), col("Players.statistics.offsides").getItem(0)).otherwise(lit(None)).cast("int").alias("Offside"),
)
fact_player_stats_df.show()
player_stats_dynamic_frame = DynamicFrame.fromDF(dim_players_df, glueContext, "player_stats_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/fact_player_stats"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = teams_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
# Extract Dim_Fixtures
dim_fixtures_df = fixtures_exploded.select(
    col("match_data.fixture.id").alias("Fixture_ID"),
    col("match_data.league.id").alias("League_ID"),
    col("match_data.league.season").alias("Season_Year"),
    col("match_data.teams.home.id").alias("Home_Team_ID"),
    col("match_data.teams.away.id").alias("Away_Team_ID"),
    col("match_data.fixture.venue.id").alias("Venue_ID"),
    to_timestamp(col("match_data.fixture.date")).alias("Match_Date"),
    to_timestamp(date_format(col("match_data.fixture.date"), "HH:mm:ss")).alias("Match_Time"),
    col("match_data.goals.home").alias("Goals_Home"),
    col("match_data.goals.away").alias("Goals_Away"),
    when(col("match_data.teams.home.winner") == True, lit(True)).otherwise(lit(False)).alias("Home_win"),
    when(col("match_data.teams.away.winner") == True, lit(True)).otherwise(lit(False)).alias("Away_win"),
    when(col("match_data.teams.home.winner") == True, col("match_data.teams.home.id"))
    .when(col("match_data.teams.away.winner") == True, col("match_data.teams.away.id"))
    .otherwise(lit(None)).alias("Winner_Team_ID"),
    col("match_data.fixture.status.short").alias("Status")
)

# Assign Season_ID by joining with Dim_Seasons
dim_fixtures_df = dim_fixtures_df.join(dim_seasons_df, dim_fixtures_df.Season_Year == dim_seasons_df.Season_Year, "left") \
                                   .select("Fixture_ID", "League_ID", "Season_ID", "Home_Team_ID", "Away_Team_ID", 
                                           "Venue_ID", "Match_Date", "Match_Time", "Goals_Home", "Goals_Away",
                                           "Home_win", "Away_win", "Winner_Team_ID", "Status")
dim_fixtures_df.show()
# Extract Fixture_ID from parameters.fixture
team_stats_df = team_stats_df.withColumn("Fixture_ID", col("parameters.fixture"))

# Explode response array to extract individual fixture records
team_stats_exploded = team_stats_df.selectExpr("Fixture_ID", "explode(response) as match_data")

# Explode statistics array for each team
team_stats_exploded = team_stats_exploded.select(
    col("Fixture_ID"),
    col("match_data.team.id").alias("Team_ID"),
    col("match_data.team.name").alias("Team_Name"),
    col("match_data.statistics").alias("statistics")
).withColumn("statistics", explode(col("statistics")))

# Explode the statistics array
fact_team_stats_df = team_stats_exploded.select(
    col("Fixture_ID"),
    col("Team_ID"),
    
    # Handle different statistics types with proper extraction and conversion
    when(col("statistics.type") == "Shots on Goal", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Shots_on_Target"),
    
    when(col("statistics.type") == "Shots off Goal", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Shots_off_Target"),
    
    when(col("statistics.type") == "Total Shots", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Total_Shots"),
    
    when(col("statistics.type") == "Blocked Shots", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Blocked_Shots"),
    
    when(col("statistics.type") == "Shots insidebox", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Shots_Inside_Box"),
    
    when(col("statistics.type") == "Shots outsidebox", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Shots_Outside_Box"),
    
    when(col("statistics.type") == "Fouls", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Fouls"),
    
    when(col("statistics.type") == "Corner Kicks", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Corner_Kicks"),
    
    when(col("statistics.type") == "Offsides", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Offsides"),
    
    # Handle percentages with cleaning
    when(col("statistics.type") == "Ball Possession", 
         when(col("statistics.value.string").rlike("^\d+%$"), regexp_replace(col("statistics.value.string"), "%", "").cast("int"))
         .otherwise(lit(None))
    ).alias("Possession_Percentage"),
    
    when(col("statistics.type") == "Yellow Cards", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Yellow_Cards"),
    
    when(col("statistics.type") == "Red Cards", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Red_Cards"),
    
    when(col("statistics.type") == "Goalkeeper Saves", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Goalkeeper_Saves"),
    
    when(col("statistics.type") == "Total passes", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Total_Passes"),
    
    when(col("statistics.type") == "Passes accurate", 
         when(col("statistics.value.int").isNotNull(), col("statistics.value.int"))
         .otherwise(when(col("statistics.value.string").rlike("^\d+$"), col("statistics.value.string").cast("int")).otherwise(lit(None)))
    ).alias("Passes_Accurate"),
    
    # Handle percentages with cleaning
    when(col("statistics.type") == "Passes %", 
         when(col("statistics.value.string").rlike("^\d+%$"), regexp_replace(col("statistics.value.string"), "%", "").cast("int"))
         .otherwise(lit(None))
    ).alias("Passes_Percent"),
    
    when(col("statistics.type") == "expected_goals", 
         when(col("statistics.value.string").rlike("^\d+\.\d+$"), col("statistics.value.string").cast("float"))
         .otherwise(lit(None))
    ).alias("Expected_Goals")
)

# Group by Fixture_ID and Team_ID to aggregate statistics
fact_team_stats_df = fact_team_stats_df.groupBy(
    "Fixture_ID", "Team_ID"
).agg(
    F.max("Shots_on_Target").alias("Shots_on_Target"),
    F.max("Shots_off_Target").alias("Shots_off_Target"),
    F.max("Total_Shots").alias("Total_Shots"),
    F.max("Blocked_Shots").alias("Blocked_Shots"),
    F.max("Shots_Inside_Box").alias("Shots_Inside_Box"),
    F.max("Shots_Outside_Box").alias("Shots_Outside_Box"),
    F.max("Fouls").alias("Fouls"),
    F.max("Corner_Kicks").alias("Corner_Kicks"),
    F.max("Offsides").alias("Offsides"),
    F.avg("Possession_Percentage").alias("Possession_Percentage"),  
    F.max("Yellow_Cards").alias("Yellow_Cards"),
    F.max("Red_Cards").alias("Red_Cards"),
    F.max("Goalkeeper_Saves").alias("Goalkeeper_Saves"),
    F.max("Total_Passes").alias("Total_Passes"),
    F.max("Passes_Accurate").alias("Passes_Accurate"),
    F.avg("Passes_Percent").alias("Passes_Percent"),  
    F.max("Expected_Goals").alias("Expected_Goals")
)
fact_team_stats_df.count()
# Join fact_team_stats_df with dim_fixtures_df on Fixture_ID
fact_team_stats_df = fact_team_stats_df.join(
    dim_fixtures_df,
    on="Fixture_ID",
    how="left"
)

# Derive Goals_Scored, Goals_Conceded, and Winner_Team
fact_team_stats_df = fact_team_stats_df.withColumn(
    "Goals_Scored",
    when(col("Team_ID") == col("Home_Team_ID"), col("Goals_Home"))
    .when(col("Team_ID") == col("Away_Team_ID"), col("Goals_Away"))
    .otherwise(lit(None))
)

fact_team_stats_df = fact_team_stats_df.withColumn(
    "Goals_Conceded",
    when(col("Team_ID") == col("Home_Team_ID"), col("Goals_Away"))
    .when(col("Team_ID") == col("Away_Team_ID"), col("Goals_Home"))
    .otherwise(lit(None))
)

fact_team_stats_df = fact_team_stats_df.withColumn(
    "Winner_Team",
    when(col("Team_ID") == col("Winner_Team_ID"), lit(True)).otherwise(lit(False))
)

# Select only relevant columns
fact_team_stats_df = fact_team_stats_df.select(
    "Fixture_ID", "Team_ID", "Goals_Scored", "Goals_Conceded", "Winner_Team",
    "Shots_on_Target", "Shots_off_Target", "Total_Shots", "Blocked_Shots",
    "Shots_Inside_Box", "Shots_Outside_Box", "Fouls", "Corner_Kicks", "Offsides",
    "Possession_Percentage", "Yellow_Cards", "Red_Cards", "Goalkeeper_Saves",
    "Total_Passes", "Passes_Accurate", "Passes_Percent", "Expected_Goals"
)
# Drop unnecessary columns from dim_fixtures_df
dim_fixtures_df = dim_fixtures_df.drop(
    "Goals_Home", "Goals_Away", "Home_win", "Away_win", "Winner_Team_ID", "Status"
)
dim_fixtures_df.show()
fixtures_dynamic_frame = DynamicFrame.fromDF(dim_fixtures_df, glueContext, "fixtures_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/dim_fixtures"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = fixtures_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
team_stats_dynamic_frame = DynamicFrame.fromDF(fact_team_stats_df, glueContext, "team_stats_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://football-data-engineering-project/processed/fact_team_stats"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = team_stats_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
job.commit()