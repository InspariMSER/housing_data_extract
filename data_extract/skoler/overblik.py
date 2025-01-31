import requests
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from typing import List
from pyspark.sql import Row

def make_request() -> dict:

    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb21haW51c2VyIjoiYW5vbnltb3VzIiwidG9rZW5pZCI6ImQ5OTQ5MGFlLTBhN2UtNDQzNS05MmYwLTMxYjcyNzgxMjZjNyIsInVzZXJpZCI6IjlkNjRjNWE4LTZjYzUtNDgyZS1hZjg1LWQ0M2FlMGRiNDcyMCIsImV4cCI6MTgzMjkyOTE5NiwiaXNzIjoiaHR0cHM6Ly9kb3RuZXRkZXRhaWwubmV0IiwiYXVkIjoiaHR0cHM6Ly9kb3RuZXRkZXRhaWwubmV0In0.U__S8TCiZFehmF6yEZNJaLaVzjzl7zeaF8jE8dIBV7I'

    """Make request to boliga.dk."""
    url = "https://api.uddannelsesstatistik.dk/Api/v1/statistik"

    headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
    }

    body = {
    "område": "GS",
    "emne": "OVER",
    "underemne": "OVERSKO",
    "nøgletal": [
        "Karaktergennemsnit",
        "Samlet elevfravær",
        "Andel med højest trivsel"
    ],
    "detaljering": [
        "[Institution].[Afdeling]",
        "[Institution].[Afdelingstype]",
        "[Institution].[Beliggenhedskommune]",
        "[Tid].[Skoleår]"
    ],
    "indlejret": False,
    "tomme_rækker": False,
    "formattering": "json",
    "side": 1
    }

    logging.info(f'Request url: {url}')
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}")
    
    return response.json()

def process_school_data(data: dict) -> List[Row]:
    """Process the raw API response into Row objects."""
    rows = []
    for item in data.get('elementer', []):
        try:
            row = Row(
                school_name=item.get('[Institution].[Afdeling]', ''),
                school_type=item.get('[Institution].[Afdelingstype]', ''),
                municipality=item.get('[Institution].[Beliggenhedskommune]', ''),
                school_year=item.get('[Tid].[Skoleår]', ''),
                grade_average=float(item.get('Karaktergennemsnit', 0)),
                absence_rate=float(item.get('Samlet elevfravær', 0)),
                wellbeing_rate=float(item.get('Andel med højest trivsel', 0))
            )
            rows.append(row)
        except Exception as e:
            logging.warning(f"Skipping row due to error: {e}")
            continue
    
    return rows

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
        .saveAsTable("mser_delta_lake.housing.skoleoverblik")
    
def ensure_schema_exists():
    """Create schema if it doesn't exist."""
    spark = SparkSession.builder.getOrCreate()
    spark.sql("CREATE CATALOG IF NOT EXISTS mser_delta_lake")
    spark.sql("CREATE SCHEMA IF NOT EXISTS mser_delta_lake.housing")


def get_school_data() -> List[Row]:
    """Get and process school data."""
    logging.info("Fetching school data from API")
    raw_data = make_request()
    
    logging.info("Processing school data")
    rows = process_school_data(raw_data)
    
    if rows:
        logging.info(f"Writing {len(rows)} rows to Delta table")
        ensure_schema_exists()
        write_to_delta(rows)
    
    return rows
