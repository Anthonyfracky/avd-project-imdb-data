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


def save_dataframe(df: DataFrame, output_path: str, format_type="csv"):
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

    full_path = f"file://{os.path.abspath(output_path)}"

    try:

        if format_type == "csv":
            df.coalesce(1).write.mode("overwrite").option(
                "header", "true").csv(output_path)
        else:
            df.coalesce(1).write.mode("overwrite").format(
                format_type).save(output_path)

        print(f"Successfully saved {row_count} rows to {output_path}")

        if os.path.exists(output_path):
            files = os.listdir(output_path)
            print(f"Files in directory: {files}")
            if not any(f for f in files if not f.startswith('.')):
                print(f"Warning: No visible files found in {output_path}")
        else:
            print(
                f"Warning: Directory {output_path} does not exist after write operation")

    except Exception as e:
        print(f"Error saving data to {output_path}: {str(e)}")

        try:
            print("Attempting alternative save method...")

            alt_path = output_path.replace('\\', '/')

            if format_type == "csv":
                df.coalesce(1).write.mode("overwrite").option(
                    "header", "true").csv(alt_path)
            else:
                df.coalesce(1).write.mode("overwrite").format(
                    format_type).save(alt_path)

            print(f"Alternative save method succeeded to {alt_path}")

            if os.path.exists(alt_path):
                files = os.listdir(alt_path)
                print(f"Files in directory: {files}")

        except Exception as inner_e:
            print(f"Alternative save method failed: {str(inner_e)}")

            try:
                print("Attempting to save as text file...")
                text_path = f"{output_path}_text"
                os.makedirs(text_path, exist_ok=True)

                df.select([df[col].cast("string") for col in df.columns]) \
                  .coalesce(1) \
                  .write.mode("overwrite") \
                  .text(text_path)

                print(f"Saved as text to {text_path}")
            except Exception as text_e:
                print(f"Text file save failed: {str(text_e)}")
                print("All save attempts failed.")
