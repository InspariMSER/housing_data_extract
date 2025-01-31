%python
%pip install requests beautifulsoup4 pandas main_dec
%restart_python

from enum import Enum
import logging
import re
from datetime import datetime, timezone
from scrape_listings import scrape_all_pages as scrape_listings
from scrape_latest_sales_prices import scrape_sales
from utils import postnumre_array

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

def validate_zip_code(zip_code: str) -> bool:
    """Validate that zip code exists in postnumre_array."""
    try:
        return int(zip_code) in postnumre_array
    except ValueError:
        return False

def get_zip_code() -> str:
    """Get and validate zip code from user."""
    while True:
        zip_code = input("Enter zip code (4 digits): ")
        if re.match(r'^\d{4}$', zip_code) and validate_zip_code(zip_code):
            return zip_code
        print("Invalid zip code. Please enter a valid 4-digit Danish zip code.")

def get_property_type() -> int:
    """Get property type from user."""
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

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create UTC timestamp at script start
    load_timestamp = datetime.now(timezone.utc)
    
    print("\nStarting housing data collection...")
    
    zip_code = get_zip_code()
    property_type = get_property_type()
    
    try:
        logging.info("Scraping current listings...")
        listings = scrape_listings(zip_code, property_type, load_timestamp)
        logging.info(f"Found {len(listings)} listings")
        
        logging.info("Scraping recent sales...")
        sales = scrape_sales(zip_code, property_type, load_timestamp)
        logging.info(f"Found {len(sales)} sales")
        
    except Exception as e:
        logging.error(f"Error processing zip code {zip_code}: {e}")
    
    print("\nCompleted housing data collection.")

if __name__ == "__main__":
    main()