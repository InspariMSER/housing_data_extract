"""Utilities for Delta table operations."""

from typing import List
import pandas as pd
from pyspark.sql import SparkSession

def init_spark() -> SparkSession:
    """Initialize Spark session."""
    return SparkSession.builder.getOrCreate()

def write_to_delta(df: pd.DataFrame, table_name: str) -> None:
    """Write DataFrame to Delta table."""
    spark = init_spark()
    spark_df = spark.createDataFrame(df)
    
    # Write to delta table
    table_path = f"mser_delta_lake.housing.{table_name}"
    spark_df.write.format("delta").mode("append").saveAsTable(table_path)

def read_from_delta(table_name: str) -> pd.DataFrame:
    """Read from Delta table into pandas DataFrame."""
    spark = init_spark()
    table_path = f"mser_delta_lake.housing.{table_name}"
    return spark.table(table_path).toPandas()
