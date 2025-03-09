from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import logging
import pandas as pd
from datetime import datetime, timedelta

# Define DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'football_data_etl',
    default_args=default_args,
    description='ETL pipeline for football data project',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Set up S3 and Redshift connection details
    AWS_CONN_ID = None  # Ensure this is set in your Airflow connection configuration
    S3_BUCKET = 'football-data-engineering-project'
    REDSHIFT_CONN_ID = 'redshift_default'  # Ensure this is set in your Airflow connection configuration
    REDSHIFT_SCHEMA = 'public'  # Adjust schema as per your Redshift setup

    # List of tables to load
    tables = [
        {'s3_key': 'processed/dim_seasons/*.csv', 'redshift_table': 'public.dim_seasons'},
        {'s3_key': 'processed/dim_countries/*.csv', 'redshift_table': 'public.dim_countries'},
        {'s3_key': 'processed/dim_leagues/*.csv', 'redshift_table': 'public.dim_leagues'},
        {'s3_key': 'processed/dim_venues/*.csv', 'redshift_table': 'public.dim_venues'},
        {'s3_key': 'processed/dim_teams/*.csv', 'redshift_table': 'public.dim_teams'},
        {'s3_key': 'processed/dim_players/*.csv', 'redshift_table': 'public.dim_players'},
        {'s3_key': 'processed/dim_fixtures/*.csv', 'redshift_table': 'public.dim_fixtures'},
        {'s3_key': 'processed/fact_player_stats/*.csv', 'redshift_table': 'public.fact_player_statistics'},
        {'s3_key': 'processed/fact_team_stats/*.csv', 'redshift_table': 'public.fact_team_statistics'},
    ]

    # Define the SQL template for copying data from S3 to Redshift
    REDSHIFT_COPY_SQL = """
        COPY {schema}.{table}
        FROM '{s3_uri}'
        IAM_ROLE '{iam_role}'
        CSV
        DELIMITER ','
        IGNOREHEADER 1
        REGION 'us-west-2';  -- Adjust the region if necessary
    """

    # Function to load data from S3 to Redshift for each table
    def load_data_from_s3_to_redshift(table_info):
        s3_uri = f"s3://{S3_BUCKET}/{table_info['s3_key']}"
        copy_sql = REDSHIFT_COPY_SQL.format(
            schema=REDSHIFT_SCHEMA,
            table=table_info['redshift_table'],
            s3_uri=s3_uri,
            iam_role=Variable.get("REDSHIFT_IAM_ROLE")  # Ensure the IAM role is stored in Airflow Variable
        )
        
        # Create a Redshift copy task for each table
        copy_task = PostgresOperator(
            task_id=f'copy_{table_info["redshift_table"]}_from_s3',
            postgres_conn_id=REDSHIFT_CONN_ID,
            sql=copy_sql,
            dag=dag,
        )
        
        return copy_task

    # Create tasks dynamically for all tables
    for table in tables:
        load_task = load_data_from_s3_to_redshift(table)
        load_task  # This will ensure the tasks are registered to the DAG