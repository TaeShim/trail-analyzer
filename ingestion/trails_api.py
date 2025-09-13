import requests
import geopandas as gpd
from shapely.geometry import LineString
import openmeteo_requests
import requests_cache
from retry_requests import retry
import datetime as dt

def api_call():
    """
    Query OpenStreetMap (Overpass API) for trail segments (ways) and peaks (nodes)
    within a fixed bounding box. Returns the raw JSON payload from Overpass.

    Notes:
      - The bounding box is [south, west, north, east] (lat/lon degrees).
      - We fetch:
          * ways tagged "highway" with values path|footway
          * nodes tagged natural=peak
      - 'out geom;' requests geometry for ways (list of lat/lon points).
    """
    # build Overpass QL query for both trails and peaks
    south, west, north, east = 50.616, -117.622, 51.860, -114.629
    bbox_str = f"{south},{west},{north},{east}"

    query = f"""
    [out:json][timeout:60];
    (
        way["highway"~"path|footway"]({bbox_str});
        node["natural"="peak"]({bbox_str});
    );
    out geom;
    """
    # POST to an Overpass instance (kumi mirror). Raises if HTTP error.
    resp = requests.post("https://overpass.kumi.systems/api/interpreter", data=query, timeout=90)
    resp.raise_for_status()
    data = resp.json()

    return data

def get_weather_for_coordinate(lat, lon, date=None):
    """
    Fetch daily weather for a single (lat, lon) using Open-Meteo.

    Args:
      lat (float): Latitude in decimal degrees.
      lon (float): Longitude in decimal degrees.
      date (datetime.date | None): Which day to fetch; defaults to yesterday.

    Returns:
      dict | None: A mapping of daily weather variables for that coordinate,
                   or None if the call fails.

    Implementation details:
      - Uses requests_cache + retry for resilience and rate-limit friendliness.
      - Open-Meteo client returns typed arrays; we extract the first (only) day.
      - sunrise/sunset are returned as epoch seconds (int64) by the SDK.
    """
    if date is None:
        date = dt.date.today() - dt.timedelta(days=1)  # Default to yesterday
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # Request a single-day daily forecast for the given point
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ["temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max", 
                 "sunrise", "sunset", "uv_index_max", "rain_sum", "showers_sum", 
                 "snowfall_sum", "precipitation_sum", "precipitation_probability_max"],
        "start_date": date,
        "end_date": date
    }
    
    try:
        # Make API call for this coordinate
        responses = openmeteo.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
        
        # Process the response (expect a single daily record)
        for response in responses:
            # Process daily data
            daily = response.Daily()
            daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
            daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
            daily_sunrise = daily.Variables(3).ValuesInt64AsNumpy()
            daily_sunset = daily.Variables(4).ValuesInt64AsNumpy()
            daily_uv_index_max = daily.Variables(5).ValuesAsNumpy()
            daily_rain_sum = daily.Variables(6).ValuesAsNumpy()
            daily_showers_sum = daily.Variables(7).ValuesAsNumpy()
            daily_snowfall_sum = daily.Variables(8).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(9).ValuesAsNumpy()
            daily_precipitation_probability_max = daily.Variables(10).ValuesAsNumpy()
            
            # Return weather data for that day (index 0) or None if missing
            return {
                "weather_date": date,
                "temperature_2m_max": daily_temperature_2m_max[0] if len(daily_temperature_2m_max) > 0 else None,
                "temperature_2m_min": daily_temperature_2m_min[0] if len(daily_temperature_2m_min) > 0 else None,
                "wind_speed_10m_max": daily_wind_speed_10m_max[0] if len(daily_wind_speed_10m_max) > 0 else None,
                "sunrise": daily_sunrise[0] if len(daily_sunrise) > 0 else None,
                "sunset": daily_sunset[0] if len(daily_sunset) > 0 else None,
                "uv_index_max": daily_uv_index_max[0] if len(daily_uv_index_max) > 0 else None,
                "rain_sum": daily_rain_sum[0] if len(daily_rain_sum) > 0 else None,
                "showers_sum": daily_showers_sum[0] if len(daily_showers_sum) > 0 else None,
                "snowfall_sum": daily_snowfall_sum[0] if len(daily_snowfall_sum) > 0 else None,
                "precipitation_sum": daily_precipitation_sum[0] if len(daily_precipitation_sum) > 0 else None,
                "precipitation_probability_max": daily_precipitation_probability_max[0] if len(daily_precipitation_probability_max) > 0 else None
            }
            
    except Exception as e:
        # Non-fatal: return None and log the issue
        print(f"Error fetching weather for lat={lat}, lon={lon}: {e}")
        return None

def get_weather_batch(coordinates, date=None):
    """
    Fetch daily weather for multiple coordinates in a single Open-Meteo call.

    Args:
      coordinates (list[tuple[float, float]]): List of (lat, lon) pairs.
      date (datetime.date | None): Day to fetch; defaults to yesterday.

    Returns:
      dict[(float,float) -> dict]: Mapping from coordinate tuple to per-day weather dict.
                                   Missing or failed coordinates are simply omitted.

    Notes:
      - Passing arrays of latitudes/longitudes lets Open-Meteo return multiple locations
        in one request, which is much faster than per-point calls.
      - Values are extracted per-response and stored keyed by the original coordinate.
    """
    if date is None:
        date = dt.date.today() - dt.timedelta(days=1)
    
    if not coordinates:
        return {}
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # Extract latitudes and longitudes (order is important — we keep the same indexing)
    latitudes = [coord[0] for coord in coordinates]
    longitudes = [coord[1] for coord in coordinates]
    
    # Request the same daily variables for all target points
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "daily": ["temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max", 
                 "sunrise", "sunset", "uv_index_max", "rain_sum", "showers_sum", 
                 "snowfall_sum", "precipitation_sum", "precipitation_probability_max"],
        "start_date": date,
        "end_date": date
    }
    
    try:
        # Make single API call for all coordinates
        responses = openmeteo.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
        
        weather_data = {}
        for i, response in enumerate(responses):
            # Defensive: some SDKs may return more responses than requested coords
            if i >= len(coordinates):
                break
                
            coord_key = coordinates[i]
            
            # Process daily data for the i-th coordinate
            daily = response.Daily()
            daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
            daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
            daily_sunrise = daily.Variables(3).ValuesInt64AsNumpy()
            daily_sunset = daily.Variables(4).ValuesInt64AsNumpy()
            daily_uv_index_max = daily.Variables(5).ValuesAsNumpy()
            daily_rain_sum = daily.Variables(6).ValuesAsNumpy()
            daily_showers_sum = daily.Variables(7).ValuesAsNumpy()
            daily_snowfall_sum = daily.Variables(8).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(9).ValuesAsNumpy()
            daily_precipitation_probability_max = daily.Variables(10).ValuesAsNumpy()
            
            # Store per-point weather (or None if arrays are empty)
            weather_data[coord_key] = {
                "weather_date": date,
                "temperature_2m_max": daily_temperature_2m_max[0] if len(daily_temperature_2m_max) > 0 else None,
                "temperature_2m_min": daily_temperature_2m_min[0] if len(daily_temperature_2m_min) > 0 else None,
                "wind_speed_10m_max": daily_wind_speed_10m_max[0] if len(daily_wind_speed_10m_max) > 0 else None,
                "sunrise": daily_sunrise[0] if len(daily_sunrise) > 0 else None,
                "sunset": daily_sunset[0] if len(daily_sunset) > 0 else None,
                "uv_index_max": daily_uv_index_max[0] if len(daily_uv_index_max) > 0 else None,
                "rain_sum": daily_rain_sum[0] if len(daily_rain_sum) > 0 else None,
                "showers_sum": daily_showers_sum[0] if len(daily_showers_sum) > 0 else None,
                "snowfall_sum": daily_snowfall_sum[0] if len(daily_snowfall_sum) > 0 else None,
                "precipitation_sum": daily_precipitation_sum[0] if len(daily_precipitation_sum) > 0 else None,
                "precipitation_probability_max": daily_precipitation_probability_max[0] if len(daily_precipitation_probability_max) > 0 else None
            }
            
        return weather_data
            
    except Exception as e:
        # Non-fatal: log and return empty dict
        print(f"Error fetching batch weather data: {e}")
        return {}

def trails_data(include_weather=True, weather_date=None, deduplicate=True, batch_size=10):      
    """
    Build a GeoDataFrame of trails from Overpass, optionally enriched with
    per-trail daily weather from Open-Meteo.

    Args:
      include_weather (bool): Whether to fetch and attach weather features.
      weather_date (datetime.date | None): Day to fetch; defaults to yesterday when None.
      deduplicate (bool): If True, group segments by trail name and keep the longest segment.
      batch_size (int): Number of coordinates per batched weather request.

    Returns:
      GeoDataFrame (EPSG:4326) with trail geometry and properties such as:
        name, surface, trail_visibility, sac_scale, start/centroid coords, weather_* fields.
    """
    data = api_call()
    trailhead_point = []
    geoms = []
    properties = []

    # Filter only trail ways that have a name and are foot-accessible
    trail_elements = [e for e in data['elements'] if e['type'] == 'way' and e['tags'].get('name') and e['tags'].get('foot') == 'yes']
    print(f"Found {len(trail_elements)} trail segments...")

    # Group by trail name to handle duplicates
    trail_groups = {}
    for element in trail_elements:
        name = element['tags']['name']
        if name not in trail_groups:
            trail_groups[name] = []
        trail_groups[name].append(element)
    
    print(f"Found {len(trail_groups)} unique trail names")
    if deduplicate:
        print(f"Processing {len(trail_groups)} unique trails (deduplicated)...")
    else:
        print(f"Processing {len(trail_elements)} trail segments (including duplicates)...")

    # Prepare to fetch weather in batches (map centroid->trail info)
    all_coordinates = []
    coordinate_to_trail = {}
    
    for trail_name, elements in trail_groups.items():
        if deduplicate:
            # Use the longest segment (most points) to represent the trail
            element = max(elements, key=lambda e: len(e['geometry']))
            elements_to_process = [element]
        else:
            elements_to_process = elements
        
        for element in elements_to_process:
            # Build list of (lon, lat) tuples for geometry
            coords = [(pt["lon"], pt["lat"]) for pt in element["geometry"]]
            # Use centroid of the line as the representative weather point
            centroid = LineString(coords).centroid
            coord_key = (centroid.y, centroid.x)  # (lat, lon) for Open-Meteo
            
            all_coordinates.append(coord_key)
            coordinate_to_trail[coord_key] = {
                'element': element,
                'elements': elements,
                'trail_name': trail_name,
                'deduplicate': deduplicate
            }

    # Fetch weather data in batches for all centroids
    weather_data_batch = {}
    if include_weather and all_coordinates:
        print(f"Fetching weather data for {len(all_coordinates)} coordinates in batches of {batch_size}...")
        
        for i in range(0, len(all_coordinates), batch_size):
            batch_coords = all_coordinates[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(all_coordinates) + batch_size - 1)//batch_size} ({len(batch_coords)} coordinates)...")
            
            batch_weather = get_weather_batch(batch_coords, weather_date)
            weather_data_batch.update(batch_weather)

    # Build per-trail property dicts and line geometries
    for coord_key, trail_info in coordinate_to_trail.items():
        element = trail_info['element']
        elements = trail_info['elements']
        trail_name = trail_info['trail_name']
        deduplicate = trail_info['deduplicate']
        
        # Original nodes for this trail segment
        coords = [(pt["lon"], pt["lat"]) for pt in element["geometry"]]
        trailhead_point.append(coords[0])
        geoms.append(LineString(coords))
        
        # Copy basic OSM tags
        trail_props = {k: element["tags"].get(k) for k in ("name","surface", "trail_visibility", "sac_scale")}
        
        # Optionally attach weather fields from the centroid lookup
        if include_weather:
            weather_data = weather_data_batch.get(coord_key)
            
            if weather_data:
                trail_props.update(weather_data)
            else:
                # If weather is missing for this centroid, include empty placeholders
                trail_props.update({
                    "weather_date": weather_date or dt.date.today() - dt.timedelta(days=1),
                    "temperature_2m_max": None,
                    "temperature_2m_min": None,
                    "wind_speed_10m_max": None,
                    "sunrise": None,
                    "sunset": None,
                    "uv_index_max": None,
                    "rain_sum": None,
                    "showers_sum": None,
                    "snowfall_sum": None,
                    "precipitation_sum": None,
                    "precipitation_probability_max": None
                })
        
        properties.append(trail_props)

    # Construct GeoDataFrame (EPSG:4326); geometry is the trail LineString
    gdf = gpd.GeoDataFrame(properties, geometry=geoms, crs="EPSG:4326")

    # Keep only valid line geometries (some ways could be malformed)
    gdf = gdf[gdf.geometry.type.isin(["LineString", "MultiLineString"])]

    return gdf
