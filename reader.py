from pyspark.sql import SparkSession
from schemas import title_basics_schema, title_ratings_schema, title_principals_schema


def read_title_basics(spark: SparkSession, path: str = "imdb-data/title.basics.tsv.gz"):
    basics = spark.read.csv(
        path,
        sep="\t",
        header=True,
        schema=title_basics_schema,
        nullValue="\\N"
    )
    print(f"Loaded title.basics with {basics.count()} rows")
    return basics


def read_title_ratings(spark: SparkSession, path: str = "imdb-data/title.ratings.tsv.gz"):
    ratings = spark.read.csv(
        path,
        sep="\t",
        header=True,
        schema=title_ratings_schema,
        nullValue="\\N"
    )
    print(f"Loaded title.ratings with {ratings.count()} rows")
    return ratings


def read_title_akas(spark: SparkSession, path: str = "imdb-data/title.akas.tsv.gz"):
    akas = spark.read.csv(
        path,
        sep="\t",
        header=True,
        nullValue="\\N"
    )
    print(f"Loaded title.akas with {akas.count()} rows")
    return akas


def read_title_principals(spark: SparkSession, path: str = "imdb-data/title.principals.tsv.gz"):
    principals = spark.read.csv(
        path,
        sep="\t",
        header=True,
        schema=title_principals_schema,
        nullValue="\\N"
    )
    print(f"Loaded title.principals with {principals.count()} rows")
    return principals


def read_title_crew(spark: SparkSession, path: str = "imdb-data/title.crew.tsv.gz"):
    crew = spark.read.csv(
        path,
        sep="\t",
        header=True,
        nullValue="\\N"
    )
    print(f"Loaded title.crew with {crew.count()} rows")
    return crew

def read_name_basics(spark: SparkSession, path: str = "imdb-data/name.basics.tsv.gz"):
    name_basics = spark.read.csv(
        path,
        sep="\t",
        header=True,
        nullValue="\\N"
    )
    print(f"Loaded name.basics with {name_basics.count()} rows")
    return name_basics