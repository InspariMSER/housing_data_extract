"""Main script to run both listing and sales price scrapers."""

import re
from enum import Enum
import logging
from scrape_listings import scrape_all_pages as scrape_listings
from scrape_latest_sales_prices import scrape_sales

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

def main():
    zip_code = get_zip_code()
    property_type = get_property_type()

    print("\nScraping current listings...")
    scrape_listings(zip_code, property_type)
    
    print("\nScraping recent sales...")
    scrape_sales(zip_code, property_type)

if __name__ == "__main__":
    main()
