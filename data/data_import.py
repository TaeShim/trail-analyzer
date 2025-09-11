import os
from dotenv import load_dotenv
from databricks import sql
import sys
import os
from pathlib import Path

def project_root():
    #When running as a script (Python script task / local)
    if "__file__" in globals():
        return Path(__file__).resolve().parents[1]

    #When running in a Databricks notebook: allow override via env,
    #Else fall back to current working directory
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()

    return Path.cwd() 

sys.path.append(str(project_root()))

from ingestion.trails_api import trails_data
from ingestion.gear_review_api import create_df


load_dotenv()

def get_databricks_connection():
    """Get Databricks connection"""
    DB_HOST = os.getenv('DB_SERVER_HOSTNAME')
    DB_HTTP = os.getenv('DB_HTTP_PATH')
    DB_TOKEN = os.getenv('DB_TOKEN')
    
    return sql.connect(server_hostname=DB_HOST, http_path=DB_HTTP, access_token=DB_TOKEN)

def insert_trails_data(include_weather=True, weather_date=None, batch_size=10):
    """
    Import trails data into the trail.trail_weather table
    """
    print("Fetching trails data...")
    trails_df = trails_data(include_weather=include_weather, weather_date=weather_date, batch_size=batch_size)
    
    print(f"Retrieved {len(trails_df)} trails")
    
    # Convert geometry to string for database storage
    trails_df['geometry'] = trails_df['geometry'].astype(str)
    
    # Prepare data for insertion
    data_to_insert = []
    for _, row in trails_df.iterrows():
        data_to_insert.append((
            row['name'],
            row['surface'],
            row['trail_visibility'],
            row['sac_scale'],
            row['start_longitude'],
            row['start_latitude'],
            row['centroid_longitude'],
            row['centroid_latitude'],
            row.get('weather_date'),
            row.get('temperature_2m_max'),
            row.get('temperature_2m_min'),
            row.get('wind_speed_10m_max'),
            row.get('sunrise'),
            row.get('sunset'),
            row.get('uv_index_max'),
            row.get('rain_sum'),
            row.get('showers_sum'),
            row.get('snowfall_sum'),
            row.get('precipitation_sum'),
            row.get('precipitation_probability_max'),
            row['geometry']
        ))
    
    # Insert data in batches
    with get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            # Switch to our catalog
            cursor.execute("USE CATALOG trails_lakehouse")
            
            # Insert new data
            insert_sql = """
            INSERT INTO trail.trail_weather 
            (name, surface, trail_visibility, sac_scale, start_longitude, start_latitude, 
             centroid_longitude, centroid_latitude, weather_date, temperature_2m_max, 
             temperature_2m_min, wind_speed_10m_max, sunrise, sunset, uv_index_max, 
             rain_sum, showers_sum, snowfall_sum, precipitation_sum, precipitation_probability_max, geometry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            batch_size = 100
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                print(f"Inserted batch {i//batch_size + 1}/{(len(data_to_insert) + batch_size - 1)//batch_size}")
            
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
    
    # Prepare data for insertion
    data_to_insert = []
    for _, row in gear_df.iterrows():
        data_to_insert.append((
            row['subreddit'],
            row['sub_name'],
            row['sub_link'],
            row['comment_created'],
            row['comment_body'],
            row['weather_season'],
            row['gear_type'],
            row['gear_brand'],
            row['brand_context'],
            row['sent_label'],
            row['sent_score']
        ))
    
    # Insert data
    with get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            # Switch to our catalog
            cursor.execute("USE CATALOG trails_lakehouse")
            
            # Insert new data
            insert_sql = """
            INSERT INTO gear.gear_reviews 
            (subreddit, sub_name, sub_link, comment_created, comment_body, 
             weather_season, gear_type, gear_brand, brand_context, sent_label, sent_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            batch_size = 100
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                print(f"Inserted batch {i//batch_size + 1}/{(len(data_to_insert) + batch_size - 1)//batch_size}")
            
            print(f"Successfully imported {len(gear_df)} gear reviews into gear.gear_reviews")

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

    """change"""
    insert_gear_reviews_data(sentiment_threshold=sentiment_threshold, sentiment_method=sentiment_method, window_words=window_words)
    
    print("\n" + "=" * 60)
    print("DATA IMPORT COMPLETED")
    print("=" * 60)

def check_data_counts():
    """
    Check the number of records in each table
    """
    with get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            # Switch to our catalog
            cursor.execute("USE CATALOG trails_lakehouse")
            
            # Check trails data
            cursor.execute("SELECT COUNT(*) as count FROM trail.trail_weather")
            trails_count = cursor.fetchone()[0]
            print(f"Trails in database: {trails_count}")
            
            # Check gear reviews data
            cursor.execute("SELECT COUNT(*) as count FROM gear.gear_reviews")
            gear_count = cursor.fetchone()[0]
            print(f"Gear reviews in database: {gear_count}")

if __name__ == "__main__":
   import_all_data()
    

