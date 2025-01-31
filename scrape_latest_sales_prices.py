"""Script for scraping sales prices from boliga.dk."""

from typing import List, TypedDict, Match
import argparse
import urllib.parse
import re
import logging
from pathlib import Path
from enum import Enum
import math

import requests
import bs4  # type: ignore
import pandas  # type: ignore
from main_dec import main
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


class Row(TypedDict):
    """A row of sales price data."""

    address: str
    zip_code: str
    city: str  # Add city field
    price: float
    date: str
    rooms: str
    m2: str
    built: str
    m2_price: float


class NoSoldListError(Exception):
    """Error used when the boliga response contains no sold list."""

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


# Update the address pattern to be more flexible
address_pattern = (r'(?P<street>[^0-9]+)(?P<number>\d+[A-Za-z]?)?'
                  r'(?:[,.]?\s*(?P<floor>(?:kl|st|[0-9]+)?\.?\s*(?:th|tv|mf|[0-9]+)?))?\s*'
                  r'(?P<zip>\d{4})\s*(?P<city>.+)')


def scrape_prices(soup: bs4.BeautifulSoup) -> List[Row]:
    """Scrape all sales prices from the sold table in a boliga response.."""
    if not soup.find_all('app-sold-list-table'):
        raise NoSoldListError()
    table = soup.find_all('app-sold-list-table')[0].table

    rows = []
    for row in table.find_all('tr'):
        columns = row.find_all('td')

        street, city = scrape_street_and_city(columns)
        zip_code = scrape_zip_code(columns)
        price    = scrape_price(columns)
        date     = scrape_date(columns)
        rooms    = scrape_rooms(columns)
        area     = scrape_area(columns)
        year     = scrape_year(columns)
        try:
            m2_price = float(price) / int(area)
        except ZeroDivisionError:
            logging.warning(f'0 area for address {street}')
            m2_price = math.nan

        rows.append(Row({
            'address' : street,
            'city': city,
            'zip_code': zip_code,
            'price'   : float(price),
            'date'    : date,
            'rooms'   : rooms,
            'm2'      : area,
            'built'   : year,
            'm2_price': m2_price
        }))
    return rows


def scrape_year(columns: bs4.element.ResultSet) -> str:
    """Scrape the build year from a row in the boliga sold table."""
    year = columns[5].find_all('span', {'class': None})[0].text.strip()
    return year


def scrape_area(columns: bs4.element.ResultSet) -> str:
    """Scrape the area in m2 from a row in the boliga sold table."""
    area = columns[3].find('span').text.strip()
    m = re.match(r'\d+', area)
    if m is None:
        raise ValueError(f'Malformed area: {area}')
    area = m[0]
    return area


def scrape_rooms(columns: bs4.element.ResultSet) -> str:
    """Scrape the no. of rooms from a row in the boliga sold table."""
    rooms = columns[4].text.strip()
    return rooms


def scrape_date(columns: bs4.element.ResultSet) -> str:
    """Scrape the sales date from a row in the boliga sold table."""
    date = columns[2].find_all(
        'span',
        {'class': 'text-nowrap'}
    )[0].text.strip()
    return date


def scrape_price(columns: bs4.element.ResultSet) -> str:
    """Scrape the sales price from a row in the boliga sold table."""
    price = columns[1].find_all(
        'span',
        {'class': 'text-nowrap'}
    )[0].text.replace(
        '\xa0',
        ''
    ).replace(
        '.',
        ''
    ).replace(
        'kr',
        ''
    ).strip()
    return price


def match_address(columns: bs4.element.ResultSet) -> Match:
    """Scrape the address from a sold table row and apply address regex."""
    address = columns[0].find(attrs={'data-gtm': 'sales_address'}).text.strip()
    m = re.match(address_pattern, address)
    if m is None:
        raise ValueError(f'Malformed address: {address}')
    return m


def replace_danish_chars(text: str) -> str:
    """Replace Danish characters with their ASCII equivalents."""
    replacements = {
        'æ': 'ae',
        'ø': 'oe',
        'å': 'aa',
        'Æ': 'Ae',
        'Ø': 'Oe',
        'Å': 'Aa'
    }
    for danish, ascii_equiv in replacements.items():
        text = text.replace(danish, ascii_equiv)
    return text

def clean_address(address: str) -> str:
    """Clean and standardize address string before parsing."""
    # Remove multiple spaces
    address = ' '.join(address.split())
    # Remove periods after floor indicators
    address = re.sub(r'(st|th|tv|mf)\.', r'\1', address)
    return address

def scrape_street_and_city(columns: bs4.element.ResultSet) -> tuple[str, str]:
    """Scrape the street name, no. and city from a row in the boliga sold table."""
    full_address = columns[0].find(attrs={'data-gtm': 'sales_address'}).text.strip()
    full_address = clean_address(full_address)
    
    # First check if there's a comma in the full address
    if ',' in full_address:
        address_parts = full_address.split(',')
        street = address_parts[0].strip()
        remaining = address_parts[1].strip()
        # Extract zip and city from remaining part
        zip_city_match = re.search(r'(\d{4})\s*(.+)', remaining)
        city = zip_city_match.group(2) if zip_city_match else remaining
        return replace_danish_chars(street), replace_danish_chars(city)
    
    # If no comma, use the regular pattern matching
    m = re.match(address_pattern, full_address)
    if m is None:
        # If pattern fails, try a simpler fallback pattern
        simple_pattern = r'^(.+?)\s+(\d{4})\s+(.+)$'
        simple_match = re.match(simple_pattern, full_address)
        if simple_match:
            street = simple_match.group(1)
            city = simple_match.group(3)
            return replace_danish_chars(street), replace_danish_chars(city)
        raise ValueError(f'Malformed address: {full_address}')
    
    street = m.group("street").strip()
    if m.group("number"):
        street += f' {m.group("number")}'
    if m.group('floor'):
        street += f' {m.group("floor")}'
    city = m.group('city').strip()
    
    return replace_danish_chars(street), replace_danish_chars(city)


def scrape_zip_code(columns: bs4.element.ResultSet) -> str:
    """Scrape the zip code from a row in the boliga sold table."""
    m = match_address(columns)
    zip_code = m.group('zip')
    return zip_code


def make_request(zip_code: str, property_type: PropertyType) -> bs4.BeautifulSoup:
    """Make request to boliga.dk."""
    url = (f'https://www.boliga.dk/salg/'
           f'resultater?searchTab=1&propertyType={property_type.value}&zipcodeFrom={zip_code}&'
           f'zipcodeTo={zip_code}&sort=date-d&page=1')
    logging.info(f'Request url: {url}')
    response = requests.get(url)
    return bs4.BeautifulSoup(response.text, features="html.parser")


def ensure_schema_exists():
    """Create schema if it doesn't exist."""
    spark = SparkSession.builder.getOrCreate()
    spark.sql("CREATE CATALOG IF NOT EXISTS mser_delta_lake")
    spark.sql("CREATE SCHEMA IF NOT EXISTS mser_delta_lake.housing")

def write_to_delta(sales: List[Row]):
    """Write sales data to Delta table."""
    if not sales:
        return
        
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(sales)
    
    # Append to existing table or create new one
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("mser_delta_lake.housing.sales_prices")

def scrape_sales(zip_code: str, property_type: int) -> List[Row]:
    """Scrape sales data for given zip code and property type."""
    property_type = PropertyType(property_type)
    soup = make_request(zip_code, property_type)
    rows = []
    try: 
        rows = scrape_prices(soup)
    except NoSoldListError:
        logging.warning(f'No results found for zip code {zip_code}')
    
    if rows:
        ensure_schema_exists()
        write_to_delta(rows)
    
    return rows