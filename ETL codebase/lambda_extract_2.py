import json
import os
import spotipy
from spotipy.oauth2 import requests
import boto3
from datetime import datetime

API_KEY = os.environ.get('API_KEY')
API_HOST = os.environ.get('API_HOST')
SEASON = os.environ.get('SEASON')
LEAGUE = os.environ.get('LEAGUE')
S3_BUCKET = os.environ.get('S3_BUCKET')
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)

def fetch_data(endpoint, params):
    """Fetches data from the Football API"""
    try:
        url = f"https://v3.football.api-sports.io/{endpoint}"
        headers = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if data and data.get("get") == endpoint:
            return data
        else:
            print(f"Invalid response from {endpoint}: {data}")
            return None
    except requests.RequestException as e:
        print(f"Error fetching {endpoint}: {e}")
        return None

def upload_to_s3(data, folder, filename):
    """Uploads JSON data to S3"""
    try:
        if data:
            json_data = json.dumps(data, indent=4, ensure_ascii=False)
            s3_key = f"to_processed/{folder}/{filename}"  # Store in respective subfolder
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json_data,
                ContentType="application/json"
            )
            print(f"Successfully uploaded {s3_key} to S3")
        else:
            print(f"No data to upload for {folder}/{filename}")
    except Exception as e:
        print(f"Error uploading {folder}/{filename} to S3: {e}")

def retrieve_player_stats(fixture_id):
    """Fetches player statistics for a given fixture"""
    return fetch_data("fixtures/players", {"fixture": fixture_id})

def retrieve_team_stats(fixture_id):
    """Fetches team statistics for a given fixture"""
    return fetch_data("fixtures/statistics", {"fixture": fixture_id})

def lambda_handler(event, context):
    """AWS Lambda entry point"""

    # Fetch up to 5 unprocessed fixture IDs
    response = table.scan(Limit=5)
    fixtures = [item["fixture_id"] for item in response.get("Items", [])]

    if not fixtures:
        return {"statusCode": 200, "body": "No new fixtures to process."}

    for fixture_id in fixtures:
        # Fetch and upload player stats
        player_stats = retrieve_player_stats(fixture_id)
        if player_stats:
            upload_to_s3(player_stats, "player_stats", f"player_stats_{fixture_id}.json")

        # Fetch and upload team stats
        team_stats = retrieve_team_stats(fixture_id)
        if team_stats:
            upload_to_s3(team_stats, "team_stats", f"team_stats_{fixture_id}.json")
        
        # Remove processed fixture from DynamoDB
        table.delete_item(Key={"fixture_id": fixture_id})

    return {
        "statusCode": 200,
        "body": json.dumps("Football data successfully extracted and uploaded to S3 subfolders!")
    }