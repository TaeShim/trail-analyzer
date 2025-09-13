# Trail Analyzer

A **data ingestion and analytics pipeline** for trail and gear review data.  
This project integrates trail geometries (via Overpass API + Open-Meteo weather) and Reddit gear reviews (via PRAW/transformers sentiment analysis) into a **Databricks Lakehouse** (Delta tables).

---

## 📌 Features

- **Trail ingestion**  
  - Queries hiking trails and peaks from [OpenStreetMap](https://www.openstreetmap.org/) via Overpass API  
  - Deduplicates trail segments by name  
  - Fetches weather features per trail centroid from [Open-Meteo](https://open-meteo.com/)  
  - Stores results in `trails_lakehouse.trail.trail_weather`

- **Gear review ingestion**  
  - Scrapes hiking/backpacking Reddit communities (`hikinggear`, `backpacking`, etc.)  
  - Extracts gear mentions, context windows, and associated seasons/conditions  
  - Runs sentiment analysis via HuggingFace `cardiffnlp/twitter-roberta-base-sentiment-latest`  
  - Stores results in `trails_lakehouse.gear.gear_reviews`

- **Databricks integration**  
  - Credentials securely retrieved from Databricks Secrets (`trailanalyzer-dev`)  
  - Writes to Delta tables for downstream analysis and Power BI visualization

  
**Libraries:
    pandas
    geopandas
    shapely
    praw
    asyncpraw
    transformers
    torch
    requests
    requests_cache
    openmeteo-requests
    retry-requests
    databricks-sql-connector
