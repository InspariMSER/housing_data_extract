# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

"""
Transform Listings with enhanced scoring algorithm.
Now includes energy class, train station distance, lot size, and other new factors.
"""

#Imports
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
import math

# COMMAND ----------

# MAGIC %md
# MAGIC # Constants

# COMMAND ----------

# Property type for filtering (1 = houses)
property_type = 1

# Energy class scoring mapping
ENERGY_CLASS_SCORES = {
    'A': 10.0,
    'B': 8.0,
    'C': 6.0,
    'D': 4.0,
    'E': 2.0,
    'F': 0.0,
    'UNKNOWN': 3.0  # Default for missing/invalid energy class
}

def normalize_energy_class(energy_class):
    """
    Normalize energy class values to handle boliga.dk's quirky data.
    - Convert to uppercase
    - Map G,H,I,J,K,L to A (these are actually A-class in boliga's system)
    - Map '-', null, empty to 'UNKNOWN'
    """
    if energy_class is None or energy_class == '' or energy_class == '-':
        return 'UNKNOWN'
    
    # Convert to uppercase and strip whitespace
    energy_class = str(energy_class).strip().upper()
    
    # Map the weird G,H,I,J,K,L values to A (these are actually A-class)
    if energy_class in ['G', 'H', 'I', 'J', 'K', 'L']:
        return 'A'
    
    # Valid energy classes
    if energy_class in ['A', 'B', 'C', 'D', 'E', 'F']:
        return energy_class
    
    # Everything else becomes UNKNOWN
    return 'UNKNOWN'

# Train stations and light rail stops accessible by bike from target zip codes
TRAIN_STATIONS = [
    {"name": "Aarhus H", "lat": 56.1496, "lon": 10.2045},
    {"name": "Skanderborg St", "lat": 55.9384, "lon": 9.9316},
    {"name": "Randers St", "lat": 56.4608, "lon": 10.0364},
    {"name": "Hadsten St", "lat": 56.3259, "lon": 10.0449},
    {"name": "Hinnerup St", "lat": 56.2827, "lon": 10.0419},
    {"name": "Langå St", "lat": 56.3889, "lon": 9.9028},
    
    # Letbane stops (Light Rail)
    {"name": "Risskov (Letbane)", "lat": 56.1836, "lon": 10.2238},
    {"name": "Skejby (Letbane)", "lat": 56.1927, "lon": 10.1722},
    {"name": "Universitetshospitalet (Letbane)", "lat": 56.1988, "lon": 10.1842},
    {"name": "Skejby Sygehus (Letbane)", "lat": 56.2033, "lon": 10.1742},
    {"name": "Lisbjerg Skole (Letbane)", "lat": 56.2178, "lon": 10.1662},
    {"name": "Lisbjerg Kirkeby (Letbane)", "lat": 56.2267, "lon": 10.1602},
    {"name": "Lystrup (Letbane)", "lat": 56.2356, "lon": 10.1542},
    {"name": "Ryomgård (Letbane)", "lat": 56.3792, "lon": 10.4928},
    {"name": "Grenaa (Letbane)", "lat": 56.4158, "lon": 10.8767}
]

MAX_DISTANCE_KM = 25.0

# Zip codes dictionary for validation
zipcodes_dict = {
    8000: "Århus C",
    8200: "Århus N",
    8210: "Århus V",
    8220: "Brabrand",
    8230: "Åbyhøj",
    8240: "Risskov",
    8250: "Egå",
    8260: "Viby J",
    8270: "Højbjerg",
    8300: "Odder",
    8310: "Tranbjerg J",
    8320: "Mårslet",
    8330: "Beder",
    8340: "Malling",
    8350: "Hundslund",
    8355: "Solbjerg",
    8361: "Hasselager",
    8362: "Hørning",
    8370: "Hadsten",
    8380: "Trige",
    8381: "Tilst",
    8382: "Hinnerup",
    8400: "Ebeltoft",
    8410: "Rønde",
    8420: "Knebel",
    8444: "Balle",
    8450: "Hammel",
    8462: "Harlev J",
    8464: "Galten",
    8471: "Sabro",
    8520: "Lystrup",
    8530: "Hjortshøj",
    8541: "Skødstrup",
    8543: "Hornslet",
    8550: "Ryomgård",
    8600: "Silkeborg",
    8660: "Skanderborg",
    8680: "Ry",
    8850: "Bjerringbro",
    8870: "Langå",
    8900: "Randers"
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

def calculate_distance_udf():
    """UDF to calculate distance to nearest train station."""
    def calculate_distance(lat, lon):
        if not lat or not lon or lat == 0 or lon == 0:
            return 0.0
            
        best_score = 0.0
        
        for station in TRAIN_STATIONS:
            # Haversine formula for great circle distance
            lat1, lon1, lat2, lon2 = math.radians(lat), math.radians(lon), math.radians(station["lat"]), math.radians(station["lon"])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            distance_km = c * 6371  # Earth radius in km
            
            # Calculate score (10 points at 0km, 0 points at MAX_DISTANCE_KM)
            if distance_km >= MAX_DISTANCE_KM:
                score = 0.0
            else:
                score = 10.0 * (1 - distance_km / MAX_DISTANCE_KM)
            
            # No weighting - all stations equal
            best_score = max(best_score, score)
        
        return round(best_score, 2)
    
    return F.udf(calculate_distance, DoubleType())

def energy_class_score_udf():
    """UDF to convert energy class to score."""
    def energy_score(energy_class):
        normalized_class = normalize_energy_class(energy_class)
        return ENERGY_CLASS_SCORES.get(normalized_class, ENERGY_CLASS_SCORES['UNKNOWN'])
    
    return F.udf(energy_score, DoubleType())

# COMMAND ----------

# Register UDFs
distance_udf = calculate_distance_udf()
energy_udf = energy_class_score_udf()

# COMMAND ----------

# MAGIC %md
# MAGIC # Action

# COMMAND ----------

# Select table, and select only the columns we want
df = spark.table("mser_catalog.housing.listings")
df = df.withColumn("full_address", F.concat_ws(" ", F.col("address_text"), F.col("house_number"), F.col("city")))

# COMMAND ----------

# Convert the dictionary to a DataFrame
zipcodes_df = spark.createDataFrame(
    [(k, v) for k, v in zipcodes_dict.items()],
    ["zip_code_filter", "zip_code_city_filter"]
)

# Create a new column to indicate if the zip_code and city match
df = df.join(zipcodes_df, df["zip_code"] == zipcodes_df["zip_code_filter"], "left")
df = df.withColumn(
    "is_in_zip_code_city",
    F.when(
        (df["city"] == df["zip_code_city_filter"]) &
        (df["zip_code"].cast("string") == df["zip_code_filter"]),
        True
    ).otherwise(False)
)

# COMMAND ----------

df = df.select(
    F.col("address_text").alias("address"),
    "house_number",
    "city",
    "full_address",
    "price",
    "m2",
    "m2_price",
    "rooms",
    "built",
    "zip_code",
    "days_on_market",
    "is_in_zip_code_city",
    # New fields
    "latitude",
    "longitude", 
    "energy_class",
    "lot_size",
    "price_change_percent",
    "is_foreclosure",
    "basement_size",
    "open_house",
    "image_urls",
    "ouId"  # Make sure we keep the ID
)
df = df.withColumn("built", F.col("built").cast("integer"))
df = df.withColumn("rooms", F.col("rooms").cast("integer"))
df = df.withColumn("m2", F.col("m2").cast("integer"))
df = df.withColumn("lot_size", F.col("lot_size").cast("integer"))
df = df.withColumn("basement_size", F.col("basement_size").cast("integer"))

# Normalize energy class using UDF
normalize_energy_udf = F.udf(normalize_energy_class, F.StringType())
df = df.withColumn("energy_class", normalize_energy_udf(F.col("energy_class")))

# COMMAND ----------

# ENHANCED SCORING ALGORITHM - POSTNUMMER SPECIFIC
# Now with 8 factors instead of 5, but scored relative to each zip code

# Create windows partitioned by zip_code for relative scoring
zip_built_window = Window.partitionBy("zip_code").orderBy(F.desc("built"))
zip_days_window = Window.partitionBy("zip_code").orderBy(F.asc("days_on_market"))
zip_m2_window = Window.partitionBy("zip_code").orderBy(F.desc("m2"))
zip_price_per_m2_window = Window.partitionBy("zip_code").orderBy(F.asc("price_per_m2"))
zip_lot_size_window = Window.partitionBy("zip_code").orderBy(F.desc("lot_size"))
zip_basement_window = Window.partitionBy("zip_code").orderBy(F.desc("basement_size"))

# Calculate price per m2 first for price efficiency scoring
df = df.withColumn("price_per_m2", F.col("price") / F.col("m2"))

# 1. Energy Class Score (NEW - High Weight) - Global scoring is fine for this
df = df.withColumn("energy_score", energy_udf(F.col("energy_class")))

# 2. Train Distance Score (NEW - High Weight) - Global scoring is fine for this
df = df.withColumn("train_distance_score", distance_udf(F.col("latitude"), F.col("longitude")))

# 3. Built Year Score (Traditional - Medium Weight) - ZIP CODE RELATIVE
df = df.withColumn("built_rank", F.dense_rank().over(zip_built_window))
df = df.withColumn("built_max_rank", F.max("built_rank").over(Window.partitionBy("zip_code")))
df = df.withColumn("built_score", 
    F.when(F.col("built_max_rank") > 1, 
           F.round(10.0 * (F.col("built_max_rank") - F.col("built_rank")) / (F.col("built_max_rank") - 1), 2))
    .otherwise(10.0)  # If all houses in zip have same build year
)

# 4. Days on Market Score (Traditional - Low Weight) - ZIP CODE RELATIVE
# Fewer days on market should get HIGHER score (rank 1 = fewest days = 10 points)
df = df.withColumn("days_rank", F.dense_rank().over(zip_days_window))
df = df.withColumn("days_max_rank", F.max("days_rank").over(Window.partitionBy("zip_code")))
df = df.withColumn("days_market_score",
    F.when(F.col("days_max_rank") > 1,
           F.round(10.0 * (F.col("days_max_rank") - F.col("days_rank")) / (F.col("days_max_rank") - 1), 2))
    .otherwise(10.0)  # If all houses in zip have same days on market
)

# 5. Size Score (Traditional - Medium Weight) - ZIP CODE RELATIVE
df = df.withColumn("size_rank", F.dense_rank().over(zip_m2_window))
df = df.withColumn("size_max_rank", F.max("size_rank").over(Window.partitionBy("zip_code")))
df = df.withColumn("size_score",
    F.when(F.col("size_max_rank") > 1,
           F.round(10.0 * (F.col("size_max_rank") - F.col("size_rank")) / (F.col("size_max_rank") - 1), 2))
    .otherwise(10.0)  # If all houses in zip have same size
)

# 6. Price Efficiency Score (Modified Traditional - Medium Weight) - ZIP CODE RELATIVE
# Lower price per m² should get HIGHER score (rank 1 = best price = 10 points)
df = df.withColumn("price_rank", F.dense_rank().over(zip_price_per_m2_window))
df = df.withColumn("price_max_rank", F.max("price_rank").over(Window.partitionBy("zip_code")))
df = df.withColumn("price_score",
    F.when(F.col("price_max_rank") > 1,
           F.round(10.0 * (F.col("price_max_rank") - F.col("price_rank")) / (F.col("price_max_rank") - 1), 2))
    .otherwise(10.0)  # If all houses in zip have same price per m2
)

# 7. Lot Size Score (NEW - Medium Weight) - ZIP CODE RELATIVE
df = df.withColumn("lot_rank", F.dense_rank().over(zip_lot_size_window))
df = df.withColumn("lot_max_rank", F.max("lot_rank").over(Window.partitionBy("zip_code")))
df = df.withColumn("lot_size_score",
    F.when(F.col("lot_max_rank") > 1,
           F.round(10.0 * (F.col("lot_max_rank") - F.col("lot_rank")) / (F.col("lot_max_rank") - 1), 2))
    .otherwise(10.0)  # If all houses in zip have same lot size
)

# 8. Basement Size Score (NEW - Low Weight) - ZIP CODE RELATIVE
df = df.withColumn("basement_rank", F.dense_rank().over(zip_basement_window))
df = df.withColumn("basement_max_rank", F.max("basement_rank").over(Window.partitionBy("zip_code")))
df = df.withColumn("basement_score",
    F.when(F.col("basement_max_rank") > 1,
           F.round(10.0 * (F.col("basement_max_rank") - F.col("basement_rank")) / (F.col("basement_max_rank") - 1), 2))
    .otherwise(10.0)  # If all houses in zip have same basement size
)

# COMMAND ----------

# TOTAL SCORE CALCULATION - NO WEIGHTING
# Simple sum of all individual scores

df = df.withColumn("total_score", F.round(
    F.col("energy_score") +           # Energy: 10 points max
    F.col("train_distance_score") +   # Train: 10 points max  
    F.col("lot_size_score") +         # Lot size: 10 points max
    F.col("size_score") +             # House size: 10 points max
    F.col("price_score") +            # Price efficiency: 10 points max
    F.col("built_score") +            # Build year: 10 points max
    F.col("basement_score") +         # Basement: 10 points max
    F.col("days_market_score"),       # Days on market: 10 points max
    2
))

# Maximum possible score: 10 + 10 + 10 + 10 + 10 + 10 + 10 + 10 = 80 points

# COMMAND ----------

# Add individual scores for transparency and debugging
df = df.select(
    "*",
    F.col("energy_score").alias("score_energy"),
    F.col("train_distance_score").alias("score_train_distance"),
    F.col("lot_size_score").alias("score_lot_size"),
    F.col("size_score").alias("score_house_size"),
    F.col("price_score").alias("score_price_efficiency"),
    F.col("built_score").alias("score_build_year"),
    F.col("basement_score").alias("score_basement"),
    F.col("days_market_score").alias("score_days_market")
)

# COMMAND ----------

# Drop temporary scoring columns to clean up
df = df.drop("energy_score", "train_distance_score", "lot_size_score", "size_score", 
             "price_score", "built_score", "basement_score", "days_market_score", 
             "price_per_m2",
             # Drop ranking columns
             "built_rank", "built_max_rank", "days_rank", "days_max_rank",
             "size_rank", "size_max_rank", "price_rank", "price_max_rank",
             "lot_rank", "lot_max_rank", "basement_rank", "basement_max_rank")

# Drop table if exists
spark.sql("DROP TABLE IF EXISTS mser_catalog.housing.listings_scored")
                   
# Write to table
df.write.mode("overwrite").saveAsTable("mser_catalog.housing.listings_scored")
