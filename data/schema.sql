CREATE CATALOG IF NOT EXISTS trails_lakehouse;
USE CATALOG trails_lakehouse;

CREATE SCHEMA IF NOT EXISTS gear;
CREATE SCHEMA IF NOT EXISTS trail;

CREATE TABLE IF NOT EXISTS gear.gear_reviews (
    subreddit STRING,
    sub_name STRING,
    sub_link STRING,
    comment_created TIMESTAMP,
    comment_body STRING,
    weather_season STRING,
    gear_type STRING,
    gear_brand STRING,
    brand_context STRING,
    sent_label STRING,
    sent_score FLOAT
);

CREATE TABLE IF NOT EXISTS trail.trail_weather (
    name STRING,
    surface STRING,
    trail_visibility STRING,
    sac_scale STRING,
    start_longitude FLOAT,
    start_latitude FLOAT,
    centroid_longitude FLOAT,
    centroid_latitude FLOAT,
    weather_date DATE,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    wind_speed_10m_max FLOAT,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    uv_index_max FLOAT,
    rain_sum FLOAT,
    showers_sum FLOAT,
    snowfall_sum FLOAT,
    precipitation_sum FLOAT,
    precipitation_probability_max FLOAT,
    geometry STRING
)