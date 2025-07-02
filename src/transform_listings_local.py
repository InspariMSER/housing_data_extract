"""
Local Housing Data Transform Script
Migrated from Databricks/PySpark to work with Pandas
Includes enhanced scoring algorithm
"""

import logging
import math
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
ENERGY_CLASS_SCORES = {
    'A': 10.0, 'B': 8.0, 'C': 6.0, 'D': 4.0, 'E': 2.0, 'F': 0.0, 'UNKNOWN': 3.0
}

# Train stations and light rail stops
TRAIN_STATIONS = [
    {"name": "Aarhus H", "lat": 56.1496, "lon": 10.2045},
    {"name": "Skanderborg St", "lat": 55.9384, "lon": 9.9316},
    {"name": "Randers St", "lat": 56.4608, "lon": 10.0364},
    {"name": "Hadsten St", "lat": 56.3259, "lon": 10.0449},
    {"name": "Hinnerup St", "lat": 56.2827, "lon": 10.0419},
    {"name": "Langå St", "lat": 56.3889, "lon": 9.9028},
    # Letbane stops (Light Rail)
    {"name": "Risskov (Letbane)", "lat": 56.1836, "lon": 10.2238},
    {"name": "Skejby (Letbane)", "lat": 56.1927, "lon": 10.1722},
    {"name": "Universitetshospitalet (Letbane)", "lat": 56.1988, "lon": 10.1842},
    {"name": "Skejby Sygehus (Letbane)", "lat": 56.2033, "lon": 10.1742},
    {"name": "Lisbjerg Skole (Letbane)", "lat": 56.2178, "lon": 10.1662},
    {"name": "Lisbjerg Kirkeby (Letbane)", "lat": 56.2267, "lon": 10.1602},
    {"name": "Lystrup (Letbane)", "lat": 56.2356, "lon": 10.1542},
    {"name": "Ryomgård (Letbane)", "lat": 56.3792, "lon": 10.4928},
    {"name": "Grenaa (Letbane)", "lat": 56.4158, "lon": 10.8767}
]

MAX_DISTANCE_KM = 25.0

# Zip codes dictionary for validation - Limited for debugging
ZIPCODES_DICT = {
    8000: "Århus C", 
    8370: "Hadsten", 
    8382: "Hinnerup",
    8270: "Højbjerg",
    8250: "Egå"
}

def normalize_energy_class(energy_class):
    """Normalize energy class values to handle boliga.dk's quirky data."""
    if pd.isna(energy_class) or energy_class == '' or energy_class == '-':
        return 'UNKNOWN'
    
    energy_class = str(energy_class).strip().upper()
    
    # Map the weird G,H,I,J,K,L values to A (these are actually A-class)
    if energy_class in ['G', 'H', 'I', 'J', 'K', 'L']:
        return 'A'
    
    # Valid energy classes
    if energy_class in ['A', 'B', 'C', 'D', 'E', 'F']:
        return energy_class
    
    # Everything else becomes UNKNOWN
    return 'UNKNOWN'

def calculate_distance_to_train(lat, lon):
    """Calculate distance to nearest train station."""
    if not lat or not lon or lat == 0 or lon == 0:
        return 0.0
        
    best_score = 0.0
    
    for station in TRAIN_STATIONS:
        # Haversine formula for great circle distance
        lat1, lon1 = math.radians(lat), math.radians(lon)
        lat2, lon2 = math.radians(station["lat"]), math.radians(station["lon"])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        distance_km = c * 6371  # Earth radius in km
        
        # Calculate score (10 points at 0km, 0 points at MAX_DISTANCE_KM)
        if distance_km >= MAX_DISTANCE_KM:
            score = 0.0
        else:
            score = 10.0 * (1 - distance_km / MAX_DISTANCE_KM)
        
        best_score = max(best_score, score)
    
    return round(best_score, 2)

def energy_class_score(energy_class):
    """Convert energy class to score."""
    normalized_class = normalize_energy_class(energy_class)
    return ENERGY_CLASS_SCORES.get(normalized_class, ENERGY_CLASS_SCORES['UNKNOWN'])

def calculate_zip_relative_score(df, zip_code, column, ascending=False):
    """Calculate relative score within zip code for a column."""
    zip_data = df[df['zip_code'] == zip_code][column]
    
    if len(zip_data) <= 1:
        return 10.0  # If only one house, give it full score
    
    # Rank the values (dense ranking)
    if ascending:
        ranks = zip_data.rank(method='dense', ascending=True)
    else:
        ranks = zip_data.rank(method='dense', ascending=False)
    
    max_rank = ranks.max()
    
    # Convert rank to score (rank 1 = best = 10 points)
    if max_rank > 1:
        score = 10.0 * (max_rank - ranks) / (max_rank - 1)
    else:
        score = 10.0
    
    return score.round(2)

def transform_listings(df: pd.DataFrame) -> pd.DataFrame:
    """Transform listings with enhanced scoring algorithm."""
    logging.info("Starting data transformation...")
    
    # Create full address
    df['full_address'] = df['address_text'] + ' ' + df['house_number'].astype(str) + ' ' + df['city']
    
    # Check if city matches zip code
    zip_df = pd.DataFrame(list(ZIPCODES_DICT.items()), columns=['zip_code_filter', 'zip_code_city_filter'])
    zip_df['zip_code_filter'] = zip_df['zip_code_filter'].astype(str)  # Convert to string to match df
    df = df.merge(zip_df, left_on='zip_code', right_on='zip_code_filter', how='left')
    df['is_in_zip_code_city'] = (df['city'] == df['zip_code_city_filter'])
    
    # Select and cast columns
    df = df[[
        'address_text', 'house_number', 'city', 'full_address', 'price', 'm2', 'm2_price',
        'rooms', 'built', 'zip_code', 'days_on_market', 'is_in_zip_code_city',
        'latitude', 'longitude', 'energy_class', 'lot_size', 'price_change_percent',
        'is_foreclosure', 'basement_size', 'open_house', 'image_urls', 'ouId'
    ]].copy()
    
    # Cast numeric columns
    numeric_cols = ['built', 'rooms', 'm2', 'lot_size', 'basement_size']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Normalize energy class
    df['energy_class'] = df['energy_class'].apply(normalize_energy_class)
    
    # Calculate price per m2 for scoring
    df['price_per_m2'] = df['price'] / df['m2']
    
    # Global scoring factors
    logging.info("Calculating global scores...")
    df['score_energy'] = df['energy_class'].apply(energy_class_score)
    df['score_train_distance'] = df.apply(lambda row: calculate_distance_to_train(row['latitude'], row['longitude']), axis=1)
    
    # Zip code relative scoring
    logging.info("Calculating zip code relative scores...")
    
    # Initialize score columns
    score_cols = ['score_lot_size', 'score_house_size', 'score_price_efficiency', 
                  'score_build_year', 'score_basement', 'score_days_market']
    for col in score_cols:
        df[col] = 0.0
    
    # Calculate scores for each zip code
    for zip_code in df['zip_code'].unique():
        zip_mask = df['zip_code'] == zip_code
        zip_df = df[zip_mask].copy()
        
        if len(zip_df) > 0:
            # Lot size score (bigger is better)
            df.loc[zip_mask, 'score_lot_size'] = calculate_zip_relative_score(df, zip_code, 'lot_size', ascending=False)
            
            # House size score (bigger is better)
            df.loc[zip_mask, 'score_house_size'] = calculate_zip_relative_score(df, zip_code, 'm2', ascending=False)
            
            # Price efficiency score (lower price per m2 is better)
            df.loc[zip_mask, 'score_price_efficiency'] = calculate_zip_relative_score(df, zip_code, 'price_per_m2', ascending=True)
            
            # Build year score (newer is better)
            df.loc[zip_mask, 'score_build_year'] = calculate_zip_relative_score(df, zip_code, 'built', ascending=False)
            
            # Basement score (bigger is better)
            df.loc[zip_mask, 'score_basement'] = calculate_zip_relative_score(df, zip_code, 'basement_size', ascending=False)
            
            # Days on market score (fewer days is better)
            df.loc[zip_mask, 'score_days_market'] = calculate_zip_relative_score(df, zip_code, 'days_on_market', ascending=True)
    
    # Calculate total score
    logging.info("Calculating total scores...")
    df['total_score'] = (
        df['score_energy'] + 
        df['score_train_distance'] + 
        df['score_lot_size'] + 
        df['score_house_size'] + 
        df['score_price_efficiency'] + 
        df['score_build_year'] + 
        df['score_basement'] + 
        df['score_days_market']
    ).round(2)
    
    # Clean up temporary columns if they exist
    cols_to_drop = ['price_per_m2']
    if 'zip_code_filter' in df.columns:
        cols_to_drop.append('zip_code_filter')
    if 'zip_code_city_filter' in df.columns:
        cols_to_drop.append('zip_code_city_filter')
    
    df = df.drop(columns=cols_to_drop)
    
    logging.info(f"Transformation complete. Records processed: {len(df)}")
    return df

if __name__ == "__main__":
    # Test with sample data
    print("Transform script ready for use")
