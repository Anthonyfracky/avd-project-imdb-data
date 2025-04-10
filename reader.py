from pyspark.sql import SparkSession
from schemas import title_basics_schema, title_ratings_schema


def read_title_basics(spark: SparkSession, path: str = "imdb-data/title.basics.tsv.gz"):
    return spark.read.csv(
        path,
        sep="\t",
        header=True,
        schema=title_basics_schema,
        nullValue="\\N"
    )


def read_title_ratings(spark: SparkSession, path: str = "imdb-data/title.ratings.tsv.gz"):
    return spark.read.csv(
        path,
        sep="\t",
        header=True,
        schema=title_ratings_schema,
        nullValue="\\N"
    )


def read_title_akas(spark: SparkSession, path: str = "imdb-data/title.akas.tsv.gz"):
    return spark.read.csv(
        path,
        sep="\t",
        header=True,
        nullValue="\\N"
    )
