# src/utils/geometry.py

import json
import pandas as pd

def convert_geojson_to_wkt(geojson_str):
    """
    Converts a GeoJSON string to WKT format.
    Currently supports Point geometry types.
    
    Args:
        geojson_str (str): GeoJSON string representation
        
    Returns:
        str: WKT formatted string
    """
    if pd.isna(geojson_str):
        return None
    
    try:
        geojson = json.loads(geojson_str)
        if geojson['type'].upper() == "POINT":
            coords = geojson['coordinates']
            return f"POINT({coords[0]} {coords[1]})"
        else:
            raise ValueError(f"Unsupported geometry type: {geojson['type']}")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing GeoJSON: {e}")
        return None

def process_geometry_columns(df):
    """
    Processes a DataFrame to convert any geometry/geography columns from GeoJSON to WKT format.
    
    Args:
        df (pandas.DataFrame): Input DataFrame
        
    Returns:
        pandas.DataFrame: DataFrame with converted geometry columns
    """
    geometry_columns = [col for col in df.columns if 'GEOMETRY' in col.upper() or 'GEOGRAPHY' in col.upper()]
    
    if not geometry_columns:
        return df
    
    df_copy = df.copy()
    for col in geometry_columns:
        df_copy[col] = df_copy[col].apply(convert_geojson_to_wkt)
    
    return df_copy