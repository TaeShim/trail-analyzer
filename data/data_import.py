import os
from databricks import sql
import sys
from pathlib import Path

if "__file__" in globals():
    # Running as a script / job: use the repo root = parent of this file's directory
    REPO_ROOT = Path(__file__).resolve().parents[1]
else:
    # Running inside a Databricks notebook: use env var if set, else hardcode your repo path
    REPO_ROOT = Path(os.getenv(
        "PROJECT_ROOT",
        "/Workspace/Users/taehyungg4@gmail.com/trail-analyzer"  # <-- adjust if needed
    )).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion.trails_api import trails_data
from ingestion.gear_review_api import create_df

# Get database credentials from Databricks secrets - dbutils should be available in Databricks environment
try:
    # Check if dbutils is available
    if 'dbutils' not in globals():
        raise NameError("dbutils is not available in this environment")
    
    # Get database credentials from Databricks secrets
    DB_SERVER_HOSTNAME  = dbutils.secrets.get("trailanalyzer-dev", "DB_SERVER_HOSTNAME")
    DB_HTTP_PATH        = dbutils.secrets.get("trailanalyzer-dev", "DB_HTTP_PATH")
    DB_TOKEN            = dbutils.secrets.get("trailanalyzer-dev", "DB_TOKEN")
    print("Successfully retrieved database credentials from Databricks secrets")
    
except NameError as e:
    raise RuntimeError(
        f"dbutils is not available in this environment: {e}. "
    )
except Exception as e:
    raise RuntimeError(
        f"Failed to retrieve database credentials from Databricks secrets: {e}. "
    )

def get_databricks_connection():
    """Get Databricks connection"""
    DB_HOST = DB_SERVER_HOSTNAME
    DB_HTTP = DB_HTTP_PATH
    DB_TOKEN = DB_TOKEN
    
    return sql.connect(server_hostname=DB_HOST, http_path=DB_HTTP, access_token=DB_TOKEN)

def insert_trails_data(include_weather=True, weather_date=None, batch_size=10):
    """
    Import trails data into the trail.trail_weather table
    """
    print("Fetching trails data...")
    trails_df = trails_data(include_weather=include_weather, weather_date=weather_date, batch_size=batch_size)
    
    print(f"Retrieved {len(trails_df)} trails")
    
    # Convert geometry to string for database storage
    pdf = trails_df.copy()
    pdf["geometry"] = pdf["geometry"].astype(str)
    sdf = spark.createDataFrame(pdf)

    sdf.write.mode("append").saveAsTable("trails_lakehouse.trail.trail_weather")
            
    print(f"Successfully imported {len(trails_df)} trails into trail.trail_weather")

def insert_gear_reviews_data(sentiment_threshold, sentiment_method, window_words):
    """
    Import gear reviews data into the gear.gear_reviews table
    
    Args:
        sentiment_threshold: Confidence threshold for sentiment classification (0.0-1.0)
        sentiment_method: "threshold" or "weighted" - method to reduce neutrals
    """
    print("Fetching gear reviews data...")
    gear_df = create_df(window_words=window_words, sentiment_threshold=sentiment_threshold, sentiment_method=sentiment_method)
    
    print(f"Retrieved {len(gear_df)} gear reviews")

    if len(gear_df) == 0:
        print("No gear reviews data to import")
        return

    pdf = gear_df.copy()
    sdf = spark.createDataframe(pdf)

    sdf.write.mode("append").saveAsTable("trails_lakehouse.gear.gear_reviews")
 
def import_all_data(include_weather=True, weather_date=None, trails_batch_size=10, 
                   sentiment_threshold=0.1, sentiment_method="positive_bias", window_words=12):
    """
    Import both trails and gear reviews data
    
    Args:
        include_weather: Whether to include weather data for trails
        weather_date: Specific date for weather data (None for current)
        trails_batch_size: Batch size for trails data processing
        sentiment_threshold: Confidence threshold for sentiment classification (0.0-1.0)
        sentiment_method: "threshold" or "weighted" - method to reduce neutrals
    """
    print("=" * 60)
    print("STARTING DATA IMPORT")
    print("=" * 60)
    
    # Import trails data
    print("\n1. Importing Trails Data...")
    print("-" * 30)
    insert_trails_data(include_weather=include_weather, weather_date=weather_date, batch_size=trails_batch_size)
    
    # Import gear reviews data
    print("\n2. Importing Gear Reviews Data...")
    print("-" * 30)

    insert_gear_reviews_data(sentiment_threshold=sentiment_threshold, sentiment_method=sentiment_method, window_words=window_words)
    
    print("\n" + "=" * 60)
    print("DATA IMPORT COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
   import_all_data()
    

