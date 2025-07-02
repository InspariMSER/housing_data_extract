"""
Local Housing Data Extraction Script
Migrated from Databricks to work with DuckDB and Pandas
"""

import logging
import json
from datetime import datetime
from typing import List, TypedDict, Tuple
from enum import Enum

import requests
import bs4
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants - Limited to 5 zip codes for debugging
ZIPCODES_DICT = {
    8000: "Århus C", 
    8370: "Hadsten", 
    8382: "Hinnerup",
    8270: "Højbjerg",
    8250: "Egå"
}

# Full list for production use:
ZIPCODES_DICT_FULL = {
    8000: "Århus C", 8200: "Århus N", 8210: "Århus V", 8220: "Brabrand",
    8230: "Åbyhøj", 8240: "Risskov", 8250: "Egå", 8260: "Viby J",
    8270: "Højbjerg", 8300: "Odder", 8310: "Tranbjerg J", 8320: "Mårslet",
    8330: "Beder", 8340: "Malling", 8350: "Hundslund", 8355: "Solbjerg",
    8361: "Hasselager", 8362: "Hørning", 8370: "Hadsten", 8380: "Trige",
    8381: "Tilst", 8382: "Hinnerup", 8400: "Ebeltoft", 8410: "Rønde",
    8420: "Knebel", 8444: "Balle", 8450: "Hammel", 8462: "Harlev J",
    8464: "Galten", 8471: "Sabro", 8520: "Lystrup", 8530: "Hjortshøj",
    8541: "Skødstrup", 8543: "Hornslet", 8550: "Ryomgård", 8600: "Silkeborg",
    8660: "Skanderborg", 8680: "Ry", 8850: "Bjerringbro", 8870: "Langå",
    8900: "Randers"
}

PROPERTY_TYPE = 1  # Houses only

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

class PropertyListing(TypedDict):
    """A row of listing data."""
    ouId: int
    address_text: str
    house_number: int
    city: str
    zip_code: str
    price: float
    rooms: float
    m2: float
    built: float
    m2_price: float
    days_on_market: int
    property_type_id: int
    property_type_name: str
    loaded_at_utc: datetime
    latitude: float
    longitude: float
    energy_class: str
    lot_size: float
    price_change_percent: float
    is_foreclosure: bool
    basement_size: float
    open_house: str
    image_urls: list

class NoListingsError(Exception):
    """Error used when the boliga response contains no listings."""
    pass

def split_address(address: str) -> Tuple[str, int]:
    """Split address into street name and house number."""
    for i, char in enumerate(address):
        if char.isdigit():
            street = address[:i].strip()
            number = address[i:].strip()
            number = ''.join(filter(str.isdigit, number))
            return street, int(number) if number else 123
    return address.strip(), 123

def replace_danish_chars(text: str) -> str:
    """Replace Danish characters with their ASCII equivalents."""
    replacements = {
        'æ': 'ae', 'ø': 'oe', 'å': 'aa',
        'Æ': 'Ae', 'Ø': 'Oe', 'Å': 'Aa'
    }
    for danish, ascii_equiv in replacements.items():
        text = text.replace(danish, ascii_equiv)
    return text

def scrape_listings(soup: bs4.BeautifulSoup) -> List[PropertyListing]:
    """Scrape all current listings from boliga response."""
    script_tag = soup.find('script', {'id': 'boliga-app-state'})
    if not script_tag or not script_tag.string:
        raise NoListingsError()

    json_str = script_tag.string.strip()
    json_str = json_str.replace('&q;', '"')

    try:
        data = json.loads(json_str)
        if not data:
            raise NoListingsError()
            
        search_results = data.get('search-service-perform')
        if not search_results:
            raise NoListingsError()
            
        results = search_results.get('results')
        if not results:
            return []

    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON data: {e}")
        raise NoListingsError()

    rows = []
    for listing in results:
        try:
            address = listing.get('street', '')
            city = listing.get('city', '')
            
            if not address or not city:
                continue

            if ',' in address:
                address_parts = address.split(',')
                address = address_parts[0].strip()
                city = address_parts[1].strip()

            address_text, house_number = split_address(address)

            ouId = int(listing.get('ouId', 0))
            zip_code = str(listing.get('zipCode', ''))
            price = float(listing.get('price', 0))
            rooms = float(listing.get('rooms', 0))
            m2 = float(listing.get('size', 0))
            built_year = float(listing.get('buildYear', 0))
            m2_price = listing.get('squaremeterPrice', 0)
            days_on_market = listing.get('daysForSale', 0)
            
            latitude = float(listing.get('latitude', 0.0))
            longitude = float(listing.get('longitude', 0.0))
            energy_class = listing.get('energyClass', '')
            lot_size = float(listing.get('lotSize', 0))
            price_change_percent = float(listing.get('priceChangePercentTotal', 0))
            is_foreclosure = bool(listing.get('isForeclosure', False))
            basement_size = float(listing.get('basementSize', 0))
            open_house = listing.get('openHouse', '')
            
            images = listing.get('images', [])
            image_urls = [img.get('url', '') for img in images if isinstance(img, dict) and img.get('url')]
            
            if not zip_code or price == 0:
                logging.warning('Skipping listing with missing zip code or price')
                continue
            
            if is_foreclosure:
                logging.info(f'Skipping foreclosure property: {address_text} {house_number}')
                continue

            rows.append(PropertyListing({
                'ouId': ouId,
                'address_text': replace_danish_chars(address_text),
                'house_number': house_number,
                'city': replace_danish_chars(city),
                'zip_code': zip_code,
                'price': price,
                'rooms': rooms,
                'm2': m2,
                'built': built_year,
                'm2_price': m2_price,
                'days_on_market': days_on_market,
                'latitude': latitude,
                'longitude': longitude,
                'energy_class': energy_class,
                'lot_size': lot_size,
                'price_change_percent': price_change_percent,
                'is_foreclosure': is_foreclosure,
                'basement_size': basement_size,
                'open_house': open_house,
                'image_urls': image_urls,
                'property_type_id': PROPERTY_TYPE,
                'property_type_name': PropertyType(PROPERTY_TYPE).name.lower(),
                'loaded_at_utc': datetime.utcnow()
            }))
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f'Error parsing listing: {e}')
            continue
            
    return rows

def make_request(zip_code: str, property_type: PropertyType, page: int = 1) -> bs4.BeautifulSoup:
    """Make request to boliga.dk listings."""
    url = f'https://www.boliga.dk/resultat?zipCodes={zip_code}&propertyType={property_type.value}&page={page}'
    response = requests.get(url)
    return bs4.BeautifulSoup(response.text, features="html.parser")

def scrape_all_pages(zip_code: str, property_type: int, loaded_at_utc: datetime) -> List[PropertyListing]:
    """Scrape all pages of listings."""
    property_type_enum = PropertyType(property_type)
    all_listings = []
    page = 1
    
    while True:
        soup = make_request(zip_code, property_type_enum, page)
        try:
            new_listings = scrape_listings(soup)
            if not new_listings:
                break
                
            for listing in new_listings:
                listing['property_type_id'] = property_type
                listing['property_type_name'] = PropertyType(property_type).name.lower()
                listing['loaded_at_utc'] = loaded_at_utc
                
            all_listings.extend(new_listings)
            page += 1
        except NoListingsError:
            break
    
    return all_listings

def extract_all_listings() -> pd.DataFrame:
    """Extract all listings and return as DataFrame."""
    logging.info("Starting data extraction...")
    loaded_at_utc = datetime.utcnow()
    all_listings = []
    
    for zip_code in ZIPCODES_DICT.keys():
        logging.info(f"Scraping zip code {zip_code}")
        listings = scrape_all_pages(str(zip_code), PROPERTY_TYPE, loaded_at_utc)
        if listings:
            logging.info(f"Scraped {len(listings)} listings for zip code {zip_code}")
            all_listings.extend(listings)
        else:
            logging.info(f"No listings found for zip code {zip_code}")
    
    if all_listings:
        df = pd.DataFrame(all_listings)
        logging.info(f"Total listings extracted: {len(df)}")
        return df
    else:
        logging.warning("No listings found")
        return pd.DataFrame()

if __name__ == "__main__":
    df = extract_all_listings()
    if not df.empty:
        print(f"Extracted {len(df)} listings")
        print(df.head())
    else:
        print("No data extracted")
