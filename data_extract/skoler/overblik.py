import requests
import bs4
import logging
import pandas as pd

def make_request() -> bs4.BeautifulSoup:

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

# Make the POST request


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


def get_overblik() -> List[Row]:
    """Scrape sales data for given zip code and property type."""
    response = make_request()
    rows = []
    
    if rows:
        ensure_schema_exists()
        write_to_delta(rows)
    
    return rows
