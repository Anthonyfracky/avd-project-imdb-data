from pyspark.sql import SparkSession, DataFrame
import os
import sys


def configure_spark_session():
    """
    Configure a Spark session with basic settings suitable for Windows environment.
    """
    return SparkSession.builder \
        .appName("IMDB Dataset") \
        .config("spark.ui.enabled", "false") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()


def save_dataframe(df: DataFrame, output_path: str):
    """
    Save a DataFrame to disk and display a sample of it.

    Args:
        df: The DataFrame to save
        output_path: Path where to save the DataFrame
        format_type: File format to use (csv, json, parquet)
    """

    output_path = os.path.normpath(output_path)

    os.makedirs(output_path, exist_ok=True)

    print(f"\n=== Writing data to {output_path} ===")
    df.show(10, truncate=False)
    row_count = df.count()
    print(f"Total rows: {row_count}")

    try:
        df.toPandas().to_csv(output_path+".csv")
    except Exception as e:
        print(f"Error saving data to {output_path}: {str(e)}")
