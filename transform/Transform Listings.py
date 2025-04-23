# Databricks notebook source
#Imports
from pyspark.sql import Window
import pyspark.sql.functions as F

# COMMAND ----------

# Create zip_code dictionary

zipcodes_dict = {
    8000: "Aarhus C",
    8200: "Aarhus N",
    8210: "Aarhus V",
    8220: "Brabrand",
    8230: "Åbyhøj",
    8240: "Risskov",
    8260: "Viby J",
    8270: "Højbjerg",
    8370: "Hadsten",
    8382: "Hinnerup",
    8520: "Lystrup",
    8530: "Hjortshøj",
    8541: "Skødstrup",
    8543: "Hornslet",
    8870: "Langå"
}

# COMMAND ----------

# Create widgets
dbutils.widgets.text("max_score", "10")
dbutils.widgets.text("max_budget", "4000000")
max_budget = int(dbutils.widgets.get("max_budget"))

# COMMAND ----------

# Select table, and select only the columns we want
df = spark.table("mser_catalog.housing.listings")
df = df.withColumn("full_address", F.concat_ws(" ", F.col("address_text"), F.col("house_number"), F.col("city")))
df = df.filter(F.col("price") <= max_budget)

# Convert the dictionary to a DataFrame
zipcodes_df = spark.createDataFrame(
    [(k, v) for k, v in zipcodes_dict.items()],
    ["zip_code_filter", "zip_code_city_filter"]
)

# Filter rows based on the zip_code and city match using a join
df = df.join(
    zipcodes_df,
    (df["zip_code"].cast("string") == zipcodes_df["zip_code_filter"]) &
    (df["city"] == zipcodes_df["zip_code_city_filter"]),
    "inner"
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
    "days_on_market"
)
df = df.withColumn("built", F.col("built").cast("integer"))
df = df.withColumn("rooms", F.col("rooms").cast("integer"))
df = df.withColumn("m2", F.col("m2").cast("integer"))

# Construct windows, that sorts different columns, by priority
built_window                = Window.orderBy(F.asc("built"))
days_on_market_window       = Window.orderBy(F.desc("days_on_market"))
m2_window                   = Window.orderBy(F.asc("m2"))
price_window                = Window.orderBy(F.desc("price"))
rooms_window                = Window.orderBy(F.asc("rooms"))

# The following variables are helping with calculating the score
# The calculation is supposed to be "The amount of distinct values in the column divided by the maxiumum score"
# This is then timed by the dense_rank
# Example:
# You have houses from 24 different built years. 
# The maximum score is 10.
# This means that the "multiplier" for points is 0,416
# A house built in 2025 (i.e. the one with higest rank) will score 10 points, while the next best will score 10 - 0,416 (9,58 points)

# Set max scoring
max_score = int(dbutils.widgets.get("max_score"))

# Create distinct count variables
distinct_built              = df.select(F.countDistinct("built")).collect()[0][0]
distinct_days_on_market     = df.select(F.countDistinct("days_on_market")).collect()[0][0]
distinct_m2                 = df.select(F.countDistinct("m2")).collect()[0][0]
distinct_price              = df.select(F.countDistinct("price")).collect()[0][0]
distinct_rooms              = df.select(F.countDistinct("rooms")).collect()[0][0]

# Calculate the multiplier for each column
built_multiplier            = max_score / distinct_built
days_on_market_multiplier   = max_score / distinct_days_on_market
m2_multiplier               = max_score / distinct_m2
price_multiplier            = max_score / distinct_price
rooms_multiplier            = max_score / distinct_rooms

# COMMAND ----------

# Here we calculate the score of each column.
df = df.withColumn("built_multiplied", F.round(F.dense_rank().over(built_window) * built_multiplier, 2))
df = df.withColumn("days_on_market_multiplied", F.round(F.dense_rank().over(days_on_market_window) * days_on_market_multiplier, 2))
df = df.withColumn("m2_multiplied", F.round(F.dense_rank().over(m2_window) * m2_multiplier, 2))
df = df.withColumn("price_multiplied", F.round(F.dense_rank().over(price_window) * price_multiplier, 2))
df = df.withColumn("rooms_multiplied", F.round(F.dense_rank().over(rooms_window) * rooms_multiplier, 2))

# COMMAND ----------

# Total the multipliers
df = df.withColumn("total_score", F.round(F.col("built_multiplied") + F.col("days_on_market_multiplied") + F.col("m2_multiplied") + F.col("price_multiplied") + F.col("rooms_multiplied"), 2))

# Drop table if exists
spark.sql("DROP TABLE IF EXISTS mser_catalog.housing.listings_scored")
                   
# Write to table
df.write.mode("overwrite").saveAsTable("mser_catalog.housing.listings_scored")
