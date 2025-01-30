%python
%pip install requests beautifulsoup4 pandas main_dec
%restart_python

from enum import Enum
import logging
from scrape_listings import scrape_all_pages as scrape_listings
from scrape_latest_sales_prices import scrape_sales
from utils import postnumre_array

# Fixed property type (1 = Hus)
PROPERTY_TYPE = 1

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

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("\nStarting housing data collection...")
    
    for zip_code in postnumre_array:
        zip_code_str = str(zip_code)
        logging.info(f"\nProcessing zip code: {zip_code_str}")
        
        try:
            logging.info("Scraping current listings...")
            listings = scrape_listings(zip_code_str, PROPERTY_TYPE)
            logging.info(f"Found {len(listings)} listings")
            
            logging.info("Scraping recent sales...")
            sales = scrape_sales(zip_code_str, PROPERTY_TYPE)
            logging.info(f"Found {len(sales)} sales")
            
        except Exception as e:
            logging.error(f"Error processing zip code {zip_code_str}: {e}")
            continue
        
    print("\nCompleted housing data collection.")

if __name__ == "__main__":
    main()