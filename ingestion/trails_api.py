import requests
import geopandas as gpd
from shapely.geometry import LineString
import openmeteo_requests
import requests_cache
from retry_requests import retry
import datetime as dt
from datetime import timedelta

def api_call():
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
    resp = requests.post("https://overpass.kumi.systems/api/interpreter", data=query, timeout=90)
    resp.raise_for_status()
    data = resp.json()

    return data

def get_weather_for_coordinate(lat, lon, date=None):
    """
    Get weather data for a single coordinate
    """
    if date is None:
        date = dt.date.today() - timedelta(days=1)  # Default to yesterday
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
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
        
        # Process the response
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
            
            # Return weather data
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
        print(f"Error fetching weather for lat={lat}, lon={lon}: {e}")
        return None

def get_weather_batch(coordinates, date=None):
    """
    Get weather data for multiple coordinates in a single API call
    Much faster than individual calls!
    """
    if date is None:
        date = dt.date.today() - dt.timedelta(days=1)
    
    if not coordinates:
        return {}
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # Extract latitudes and longitudes
    latitudes = [coord[0] for coord in coordinates]
    longitudes = [coord[1] for coord in coordinates]
    
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
            if i >= len(coordinates):
                break
                
            coord_key = coordinates[i]
            
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
        print(f"Error fetching batch weather data: {e}")
        return {}

def trails_data(include_weather=True, weather_date=None, deduplicate=True, batch_size=10):      
    data = api_call()
    trailhead_point = []
    geoms = []
    properties = []

    # Filter elements first
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

    # Collect all coordinates for batch weather fetching
    all_coordinates = []
    coordinate_to_trail = {}
    
    for trail_name, elements in trail_groups.items():
        if deduplicate:
            # Use the longest segment (most coordinates) as the representative
            element = max(elements, key=lambda e: len(e['geometry']))
            elements_to_process = [element]
        else:
            elements_to_process = elements
        
        for element in elements_to_process:
            coords = [(pt["lon"], pt["lat"]) for pt in element["geometry"]]
            centroid = LineString(coords).centroid
            coord_key = (centroid.y, centroid.x)  # (lat, lon)
            
            all_coordinates.append(coord_key)
            coordinate_to_trail[coord_key] = {
                'element': element,
                'elements': elements,
                'trail_name': trail_name,
                'deduplicate': deduplicate
            }

    # Fetch weather data in batches
    weather_data_batch = {}
    if include_weather and all_coordinates:
        print(f"Fetching weather data for {len(all_coordinates)} coordinates in batches of {batch_size}...")
        
        for i in range(0, len(all_coordinates), batch_size):
            batch_coords = all_coordinates[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(all_coordinates) + batch_size - 1)//batch_size} ({len(batch_coords)} coordinates)...")
            
            batch_weather = get_weather_batch(batch_coords, weather_date)
            weather_data_batch.update(batch_weather)

    # Process trails and add weather data
    for coord_key, trail_info in coordinate_to_trail.items():
        element = trail_info['element']
        elements = trail_info['elements']
        trail_name = trail_info['trail_name']
        deduplicate = trail_info['deduplicate']
        
        coords = [(pt["lon"], pt["lat"]) for pt in element["geometry"]]
        trailhead_point.append(coords[0])
        geoms.append(LineString(coords))
        
        # Get trail properties
        trail_props = {k: element["tags"].get(k) for k in ("name","surface", "trail_visibility", "sac_scale")}
        
        # Add segment info if not deduplicating
        if not deduplicate and len(elements) > 1:
            trail_props["segment_count"] = len(elements)
            trail_props["segment_id"] = elements.index(element) + 1
        
        # Add longitude information
        trail_props["start_longitude"] = coords[0][0]  # Starting longitude
        trail_props["start_latitude"] = coords[0][1]   # Starting latitude
        
        # Calculate centroid longitude and latitude
        centroid = LineString(coords).centroid
        trail_props["centroid_longitude"] = centroid.x
        trail_props["centroid_latitude"] = centroid.y
        
        # Add weather data if requested
        if include_weather:
            weather_data = weather_data_batch.get(coord_key)
            
            if weather_data:
                trail_props.update(weather_data)
            else:
                # Add empty weather fields if API call failed
                trail_props.update({
                    "weather_date": weather_date or dt.date.today() - timedelta(days=1),
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

    gdf = gpd.GeoDataFrame(properties, geometry=geoms, crs="EPSG:4326")

    # filter just in case
    gdf = gdf[gdf.geometry.type.isin(["LineString", "MultiLineString"])]

    return gdf

def peaks_data():
    data = api_call()
    peaks = []
    
    for element in data["elements"]:
        if element["type"] == "node" and "lat" in element and "lon" in element:
            peaks.append((element["lon"], element["lat"]))
    
    return peaks
