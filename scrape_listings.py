"""Script for scraping current property listings from boliga.dk."""

from typing import List, TypedDict, Match
import re
import logging
from pathlib import Path
from enum import Enum
from datetime import datetime

import requests
import bs4  # type: ignore
import pandas  # type: ignore
from main_dec import main
import json  # Add this import at the top of the file
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

class PropertyListing(TypedDict):
    """A row of listing data."""
    address: str
    zip_code: str
    price: float
    rooms: str
    m2: str
    built: str
    m2_price: float
    property_type_id: int
    property_type_name: str
    loaded_at_utc: datetime

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
    if not script_tag or not script_tag.string:
        raise NoListingsError()

    # Clean up the JSON string
    json_str = script_tag.string.strip()
    json_str = json_str.replace('&q;', '"')  # Replace &q; with "

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
            # Get values with None fallbacks
            address = listing.get('street', '')
            city = listing.get('city', '')
            
            # Skip listings with missing required data
            if not address or not city:
                continue

            # Handle addresses with commas
            if ',' in address:
                address_parts = address.split(',')
                address = address_parts[0].strip()
                city = address_parts[1].strip()

            # Get other fields with safe fallbacks
            zip_code = str(listing.get('zipCode', ''))
            price = float(listing.get('price', 0))
            rooms = str(listing.get('rooms', ''))
            m2 = str(listing.get('size', ''))
            built_year = str(listing.get('buildYear', ''))
            m2_price = listing.get('squaremeterPrice', 0)
            days_on_market = listing.get('daysForSale', 0)
            
            # Skip listings with missing critical data
            if not zip_code or price == 0:
                logging.warning(f'Skipping listing with missing zip code or price')
                continue

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
        except (KeyError, ValueError, TypeError) as e:
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

def ensure_schema_exists():
    """Create schema if it doesn't exist."""
    spark = SparkSession.builder.getOrCreate()
    spark.sql("CREATE CATALOG IF NOT EXISTS mser_delta_lake")
    spark.sql("CREATE SCHEMA IF NOT EXISTS mser_delta_lake.housing")

def write_to_delta(listings: List[PropertyListing]):
    """Write listings to Delta table."""
    if not listings:
        return
        
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(listings)
        
    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("mser_delta_lake.housing.listings")

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
                
            # Add property type and timestamp to each listing
            for listing in new_listings:
                listing['property_type_id'] = property_type
                listing['property_type_name'] = PropertyType(property_type).name.lower()
                listing['loaded_at_utc'] = loaded_at_utc
                
            all_listings.extend(new_listings)
            page += 1
        except NoListingsError:
            break
    
    if not all_listings:
        print(f"No listings found for zip code {zip_code}")
    else:
        ensure_schema_exists()
        write_to_delta(all_listings)
    
    return all_listings

def format_filename(zip_code: str) -> str:
    """Format the output csv file name."""
    return f'listings_{zip_code}.csv'