%reload_ext autoreload
from pyspark.sql import functions as F
import os
from databricks import sql
import sys
from pathlib import Path
import pandas as pd

# Resolve project root for both script/jobs and interactive Databricks notebooks
if "__file__" in globals():
    # Running as a script / job: use the repo root = parent of this file's directory
    REPO_ROOT = Path(__file__).resolve().parents[1]
else:
    # Running inside a Databricks notebook: use env var if set, else hardcode your repo path
    REPO_ROOT = Path(os.getenv(
        "PROJECT_ROOT",
        "/Workspace/Users/taehyungg4@gmail.com/trail-analyzer"  # <-- adjust if needed
    )).resolve()

# Ensure project root is importable (so ingestion.* modules can be imported)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion.trails_api import trails_data
from ingestion.gear_review_api import create_df

# --- Secrets / credentials: pulled from Databricks secret scopes via dbutils ---
# This block assumes the code is running on Databricks (dbutils available).
# We surface a helpful error if run outside that environment.
try:
    # Check if dbutils is available
    if 'dbutils' not in globals():
        raise NameError("dbutils is not available in this environment")
    
    # Get database + Reddit API credentials from the 'trailanalyzer-dev' scope
    DB_SERVER_HOSTNAME  = dbutils.secrets.get("trailanalyzer-dev", "DB_SERVER_HOSTNAME")
    DB_HTTP_PATH        = dbutils.secrets.get("trailanalyzer-dev", "DB_HTTP_PATH")
    DB_TOKEN            = dbutils.secrets.get("trailanalyzer-dev", "DB_TOKEN")
    CLIENT              = dbutils.secrets.get("trailanalyzer-dev", "REDDIT_CLIENT_ID")
    SECRET              = dbutils.secrets.get("trailanalyzer-dev", "REDDIT_SECRET")
    USER_AGENT          = dbutils.secrets.get("trailanalyzer-dev", "REDDIT_USER_AGENT")
    print("Successfully retrieved database credentials from Databricks secrets")
    
except NameError as e:
    # Running off-Databricks: fail fast with a descriptive message
    raise RuntimeError(
        f"dbutils is not available in this environment: {e}. "
    )
except Exception as e:
    # Any other failure retrieving secrets: bubble up with context
    raise RuntimeError(
        f"Failed to retrieve database credentials from Databricks secrets: {e}. "
    )

def insert_trails_data(include_weather=True, weather_date=None, batch_size=10):
    """
    Fetch trail geometries + optional weather (from ingestion.trails_api.trails_data),
    normalize types, then append to Delta table trails_lakehouse.trail.trail_weather.
    
    Args:
        include_weather (bool): join Open-Meteo daily features per trail centroid
        weather_date (date|None): target day; None defaults to yesterday in the API
        batch_size (int): batch size for weather API calls inside trails_data
    """
    print("Fetching trails data...")
    trails_df = trails_data(include_weather=include_weather, weather_date=weather_date, batch_size=batch_size)
    
    print(f"Retrieved {len(trails_df)} trails")
    
    # Convert GeoDataFrame -> pandas, and ensure 'geometry' is serialized as text for storage
    pdf = trails_df.copy()
    pdf["geometry"] = pdf["geometry"].astype(str)

    # Convert epoch-like sunrise/sunset columns (returned as int64 from SDK) into UTC timestamps
    for col in pdf[["sunrise", "sunset"]]:
        if col in pdf.columns:
            pdf[col] = pd.to_datetime(pdf[col], unit="s", errors="coerce", utc=True)
            
    # Ensure weather_date is a date (no time component) if present
    if "weather_date" in pdf.columns:
        pdf["weather_date"] = pd.to_datetime(pdf["weather_date"], errors="coerce").dt.date

    # Create Spark DataFrame from pandas and append to Delta
    sdf = spark.createDataFrame(pdf)

    # NOTE: Assumes destination Delta table exists with compatible schema
    sdf.write.mode("append").saveAsTable("trails_lakehouse.trail.trail_weather")
            
    print(f"Successfully imported {len(trails_df)} trails into trail.trail_weather")

def insert_gear_reviews_data(sentiment_threshold, sentiment_method, window_words, client, secret, agent):
    """
    Scrape gear reviews/comments via Reddit (create_df), then append to Delta table
    trails_lakehouse.gear.gear_reviews. Casts sentiment columns to match table schema.
    
    Args:
        sentiment_threshold (float): Confidence cutoff used in custom sentiment logic
        sentiment_method (str): "threshold", "weighted", or "positive_bias"
        window_words (int): context window size around brand mentions
        client/secret/agent (str): Reddit API credentials
    """
    print("Fetching gear reviews data...")
    
    # Pull a pandas DataFrame of reviews + derived sentiment features
    gear_df = create_df(window_words=window_words, sentiment_threshold=sentiment_threshold, sentiment_method=sentiment_method, client=client, secret=secret, agent=agent)
    
    print(f"Retrieved gear reviews")

    # If no rows, exit early (avoid creating empty partitions)
    if len(gear_df) == 0:
        print("No gear reviews data to import")
        return

    pdf = gear_df.copy()
            
    # Pandas -> Spark
    sdf = spark.createDataFrame(pdf)

    # Cast to match Delta table types (e.g., sent_score FLOAT, sent_label STRING)
    sdf = (
    sdf.withColumn("sent_label", F.col("sent_label").cast("string"))
       .withColumn("sent_score", F.col("sent_score").cast("float"))
        )

    # Append to target table
    sdf.write.mode("append").saveAsTable("trails_lakehouse.gear.gear_reviews")
 
def import_all_data(include_weather=True, weather_date=None, trails_batch_size=10, 
                   sentiment_threshold=0.1, sentiment_method="positive_bias", window_words=12):
    """
    Orchestrate both trail and gear loads in sequence.
    
    Args:
        include_weather (bool): pass-through to insert_trails_data
        weather_date (date|None): pass-through to insert_trails_data
        trails_batch_size (int): pass-through to insert_trails_data
        sentiment_threshold (float): pass-through to insert_gear_reviews_data
        sentiment_method (str): pass-through to insert_gear_reviews_data
        window_words (int): pass-through to insert_gear_reviews_data
    """
    print("=" * 60)
    print("STARTING DATA IMPORT")
    print("=" * 60)
    
    # 1) Trails pipeline
    print("\n1. Importing Trails Data...")
    print("-" * 30)
    insert_trails_data(include_weather=include_weather, weather_date=weather_date, batch_size=trails_batch_size)
    
    # 2) Gear reviews pipeline
    print("\n2. Importing Gear Reviews Data...")
    print("-" * 30)

    insert_gear_reviews_data(sentiment_threshold=sentiment_threshold, sentiment_method=sentiment_method, window_words=window_words, client=CLIENT, secret=SECRET, agent=USER_AGENT)
    
    print("\n" + "=" * 60)
    print("DATA IMPORT COMPLETED")
    print("=" * 60)

# Entry point if executed as a script (jobs)
if __name__ == "__main__":
   import_all_data()
