"""
Local Housing Data Database Management
Uses DuckDB for local storage and querying
"""

import logging
from pathlib import Path
import duckdb
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HousingDataDB:
    def __init__(self, db_path: str = "data/housing.duckdb"):
        """Initialize database connection."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_tables()
    
    def _init_tables(self):
        """Initialize database tables."""
        logging.info("Initializing database tables...")
        
        # Create listings table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                ouId INTEGER PRIMARY KEY,
                address_text VARCHAR,
                house_number INTEGER,
                city VARCHAR,
                zip_code VARCHAR,
                price DOUBLE,
                rooms DOUBLE,
                m2 DOUBLE,
                built DOUBLE,
                m2_price INTEGER,
                days_on_market INTEGER,
                latitude DOUBLE,
                longitude DOUBLE,
                energy_class VARCHAR,
                lot_size DOUBLE,
                price_change_percent DOUBLE,
                is_foreclosure BOOLEAN,
                basement_size DOUBLE,
                open_house VARCHAR,
                image_urls VARCHAR,
                property_type_id INTEGER,
                property_type_name VARCHAR,
                loaded_at_utc TIMESTAMP
            )
        """)
        
        # Create listings_scored table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS listings_scored (
                address_text VARCHAR,
                house_number INTEGER,
                city VARCHAR,
                full_address VARCHAR,
                price DOUBLE,
                m2 INTEGER,
                m2_price INTEGER,
                rooms INTEGER,
                built INTEGER,
                zip_code VARCHAR,
                days_on_market INTEGER,
                is_in_zip_code_city BOOLEAN,
                latitude DOUBLE,
                longitude DOUBLE,
                energy_class VARCHAR,
                lot_size INTEGER,
                price_change_percent DOUBLE,
                is_foreclosure BOOLEAN,
                basement_size INTEGER,
                open_house VARCHAR,
                image_urls VARCHAR,
                ouId INTEGER PRIMARY KEY,
                score_energy DOUBLE,
                score_train_distance DOUBLE,
                score_lot_size DOUBLE,
                score_house_size DOUBLE,
                score_price_efficiency DOUBLE,
                score_build_year DOUBLE,
                score_basement DOUBLE,
                score_days_market DOUBLE,
                total_score DOUBLE
            )
        """)
        
        # Create seen_houses table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_houses (
                ouId INTEGER PRIMARY KEY,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        logging.info("Database tables initialized")
    
    def truncate_listings(self):
        """Clear all listings data."""
        self.conn.execute("DELETE FROM listings")
        self.conn.execute("DELETE FROM listings_scored")
        logging.info("Listings tables truncated")
    
    def drop_and_recreate_tables(self):
        """Drop existing tables and recreate them with current schema."""
        self.conn.execute("DROP TABLE IF EXISTS listings")
        self.conn.execute("DROP TABLE IF EXISTS listings_scored")
        self.conn.execute("DROP TABLE IF EXISTS seen_houses")
        self._init_tables()
        logging.info("Tables dropped and recreated")
    
    def insert_listings(self, df: pd.DataFrame):
        """Insert listings data."""
        if df.empty:
            logging.warning("No data to insert")
            return
        
        # Convert image_urls list to string for storage
        df_copy = df.copy()
        if 'image_urls' in df_copy.columns:
            df_copy['image_urls'] = df_copy['image_urls'].astype(str)
        
        # Insert using DuckDB's DataFrame integration
        self.conn.execute("INSERT OR REPLACE INTO listings SELECT * FROM df_copy")
        logging.info(f"Inserted {len(df)} listings")
    
    def insert_scored_listings(self, df: pd.DataFrame):
        """Insert scored listings data."""
        if df.empty:
            logging.warning("No scored data to insert")
            return
            
        # Convert image_urls list to string for storage
        df_copy = df.copy()
        if 'image_urls' in df_copy.columns:
            df_copy['image_urls'] = df_copy['image_urls'].astype(str)
        
        # Insert using DuckDB's DataFrame integration
        self.conn.execute("INSERT OR REPLACE INTO listings_scored SELECT * FROM df_copy")
        logging.info(f"Inserted {len(df)} scored listings")
    
    def get_listings(self) -> pd.DataFrame:
        """Get all listings."""
        return self.conn.execute("SELECT * FROM listings").df()
    
    def get_scored_listings(self) -> pd.DataFrame:
        """Get all scored listings."""
        return self.conn.execute("SELECT * FROM listings_scored").df()
    
    def get_seen_houses(self) -> pd.DataFrame:
        """Get seen houses."""
        return self.conn.execute("SELECT * FROM seen_houses").df()
    
    def add_seen_house(self, ou_id: int):
        """Add a house to seen list."""
        self.conn.execute(
            "INSERT OR REPLACE INTO seen_houses (ouId) VALUES (?)", 
            [ou_id]
        )
        logging.info(f"Added house {ou_id} to seen list")
    
    def get_listings_for_streamlit(self) -> pd.DataFrame:
        """Get listings formatted for Streamlit app."""
        query = """
        SELECT 
            full_address, price, m2, m2_price, rooms, 
            CAST(built AS VARCHAR) as built,
            zip_code, days_on_market, is_in_zip_code_city, 
            total_score, ouId,
            energy_class, lot_size, basement_size, 
            score_energy, score_train_distance, score_lot_size, 
            score_house_size, score_price_efficiency, score_build_year,
            score_basement, score_days_market
        FROM listings_scored
        """
        return self.conn.execute(query).df()
    
    def close(self):
        """Close database connection."""
        self.conn.close()
        logging.info("Database connection closed")

def main():
    """Test database functionality."""
    db = HousingDataDB()
    
    # Test with sample data
    sample_data = pd.DataFrame([{
        'ouId': 12345,
        'address_text': 'Test Street',
        'house_number': 1,
        'city': 'Aarhus C',
        'full_address': 'Test Street 1 Aarhus C',
        'price': 1000000.0,
        'm2': 100,
        'm2_price': 10000,
        'rooms': 4,
        'built': 2000,
        'zip_code': '8000',
        'days_on_market': 30,
        'is_in_zip_code_city': True,
        'latitude': 56.1496,
        'longitude': 10.2045,
        'energy_class': 'B',
        'lot_size': 500,
        'price_change_percent': 0.0,
        'is_foreclosure': False,
        'basement_size': 50,
        'open_house': '',
        'image_urls': '[]',
        'property_type_id': 1,
        'property_type_name': 'hus',
        'loaded_at_utc': datetime.now()
    }])
    
    db.insert_listings(sample_data)
    print("Sample data inserted successfully")
    
    db.close()

if __name__ == "__main__":
    main()
