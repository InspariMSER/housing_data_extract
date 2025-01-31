"""Main script to run both listing and sales price scrapers."""

import re
from enum import Enum
import logging
from scrape_listings import scrape_all_pages as scrape_listings
from scrape_latest_sales_prices import scrape_sales
from utils import zipcodes
from pyspark.sql import SparkSession

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

def get_property_type() -> int:
    """Get property type from user input using integer values."""
    print("\nAvailable property types:")
    for prop_type in PropertyType:
        print(f"{prop_type.value}: {prop_type.name}")
    
    while True:
        try:
            choice = int(input("\nEnter property type number: "))
            if 1 <= choice <= 10:
                return choice
            print("Please enter a number between 1 and 10.")
        except ValueError:
            print("Please enter a valid number.")

def init_spark():
    """Initialize Spark session."""
    return SparkSession.builder \
        .appName("Housing Data Extract") \
        .getOrCreate()

def main():
    spark = init_spark()
    property_type = get_property_type()

    # Drop and recreate table
    spark.sql("DROP TABLE IF EXISTS mser_delta_lake.housing.listings")
    
    for zip_code in zipcodes:
        zip_code_str = str(zip_code)
        print(f"\nProcessing zip code: {zip_code_str}")
        
        try:
            print("Scraping current listings...")
            scrape_listings(zip_code_str, property_type)
            
            print("Scraping recent sales...")
            scrape_sales(zip_code_str, property_type)

            print("Done with zip code: {zip_code_str}")
        except Exception as e:
            logging.error(f"Error processing zip code {zip_code_str}: {str(e)}")
            continue

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()