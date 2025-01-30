

"""Script for scraping current property listings from boliga.dk."""

from typing import List, TypedDict, Match
import re
import logging
from pathlib import Path
from enum import Enum

import requests
import bs4  # type: ignore
import pandas  # type: ignore
from main_dec import main
import json  # Add this import at the top of the file

class PropertyListing(TypedDict):
    """A row of listing data."""
    address: str
    zip_code: str
    price: float
    rooms: str
    m2: str
    built: str
    m2_price: float

class NoListingsError(Exception):
    """Error used when the boliga response contains no listings."""
    pass

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

def scrape_listings(soup: bs4.BeautifulSoup) -> List[PropertyListing]:
    """Scrape all current listings from boliga response."""
    script_tag = soup.find('script', {'id': 'boliga-app-state'})
    if not script_tag:
        raise NoListingsError()

    # Clean up the JSON string
    json_str = script_tag.string.strip()
    json_str = json_str.replace('&q;', '"')  # Replace &q; with "

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON data")
        raise NoListingsError()

    results = data.get('search-service-perform', {}).get('results', [])

    rows = []
    for listing in results:
        try:
            address = listing['street']
            city = listing['city']
            
            # Handle addresses with commas
            if ',' in address:
                address_parts = address.split(',')
                address = address_parts[0].strip()
                city = address_parts[1].strip()

            zip_code = str(listing['zipCode'])
            price = float(listing['price'])
            rooms = str(listing['rooms'])
            m2 = str(listing['size'])
            built_year = str(listing['buildYear'])
            m2_price = listing['squaremeterPrice']
            days_on_market = listing['daysForSale']
            
            rows.append(PropertyListing({
                'address': replace_danish_chars(address),
                'city': replace_danish_chars(city),
                'zip_code': zip_code,
                'price': price,
                'rooms': rooms,
                'm2': m2,
                'built': built_year,
                'm2_price': m2_price,
                'days_on_market': days_on_market
            }))
        except (KeyError, ValueError) as e:
            logging.warning(f'Error parsing listing: {e}')
            continue
            
    return rows

def replace_danish_chars(text: str) -> str:
    """Replace Danish characters with their ASCII equivalents."""
    replacements = {
        'æ': 'ae', 'ø': 'oe', 'å': 'aa',
        'Æ': 'Ae', 'Ø': 'Oe', 'Å': 'Aa'
    }
    for danish, ascii_equiv in replacements.items():
        text = text.replace(danish, ascii_equiv)
    return text

def make_request(zip_code: str, property_type: PropertyType, page: int = 1) -> bs4.BeautifulSoup:
    """Make request to boliga.dk listings."""
    url = f'https://www.boliga.dk/resultat?zipCodes={zip_code}&propertyType={property_type.value}&page={page}'
    logging.info(f'Request url: {url}')
    response = requests.get(url)
    return bs4.BeautifulSoup(response.text, features="html.parser")

def scrape_all_pages(zip_code: str, property_type: int) -> List[PropertyListing]:
    """Scrape all pages of listings."""
    property_type = PropertyType(property_type)  # Convert int to enum
    all_listings = []
    page = 1
    while True:
        soup = make_request(zip_code, property_type, page)
        try:
            new_listings = scrape_listings(soup)
            if not new_listings:
                break
            all_listings.extend(new_listings)
            page += 1
        except NoListingsError:
            break
    
    if not all_listings:
        print(f"No listings found for zip code {zip_code}")
    else:
        df = pandas.DataFrame(all_listings)
        from delta_utils import write_to_delta
        write_to_delta(df, "listings")
    
    return all_listings

def format_filename(zip_code: str) -> str:
    """Format the output csv file name."""
    return f'listings_{zip_code}.csv'