"""Main script to run both listing and sales price scrapers."""
%pip install pandas requests beautifulsoup4 main_dec

import re
from enum import Enum
import logging
from scrape_listings import scrape_all_pages as scrape_listings
from scrape_latest_sales_prices import scrape_sales
from utils import zipcodes
from pyspark.sql import SparkSession
from datetime import datetime

class PropertyType(Enum):
    Hus = 1
    Raekkehus = 2
    Ejerlejlighed = 3
    Fritidshus = 4
    Andelsbolig = 5
    Landejendom = 6
    Helårsgrund = 7
    Fritidsgrund = 8
    Villalejlighed = 9
    Andet = 10

def get_zip_code() -> str:
    """Get zip code from user input."""
    while True:
        zip_code = input("Enter zip code (4 digits): ")
        if re.match(r'^\d{4}$', zip_code):
            return zip_code
        print("Invalid zip code. Please enter 4 digits.")

def init_spark():
    """Initialize Spark session."""
    return SparkSession.builder \
        .appName("Housing Data Extract") \
        .getOrCreate()

def main():
    spark = init_spark()
    property_type = 1  # Fixed to "Hus"
    loaded_at_utc = datetime.utcnow()
    
    # Drop and recreate table
    spark.sql("DROP TABLE IF EXISTS mser_delta_lake.housing.listings")
    
    for zip_code in zipcodes:
        zip_code_str = str(zip_code)
        print(f"\nProcessing zip code: {zip_code_str}")
        
        try:
            print("Scraping current listings...")
            scrape_listings(zip_code_str, property_type, loaded_at_utc)
            
            print("Scraping recent sales...")
            scrape_sales(zip_code_str, property_type, loaded_at_utc)
            
            print(f"Done with zip code: {zip_code_str}")
        except Exception as e:
            logging.error(f"Error processing zip code {zip_code_str}: {str(e)}")
            continue

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()