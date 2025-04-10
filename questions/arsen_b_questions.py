from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, rank, count, avg
from utils import save_dataframe


# === FILTERS ===

def filter_ukrainian_titles(df: DataFrame):
    """
    Filter all movies available in Ukrainian language.
    """
    result = df.filter((col("language") == "uk"))
    print("\n=== [FILTER] Titles in Ukrainian language ===")
    save_dataframe(result, "outputs/arsen_b/filter_ukrainian_titles")


def filter_recent_short_movies(df: DataFrame):
    """
    Filter short movies released after 2015.
    """
    result = df.filter(
        (col("titleType") == "short") &
        (col("startYear") >= 2015) &
        (col("runtimeMinutes").isNotNull()) &
        (col("runtimeMinutes") < 40)
    )
    print("\n=== [FILTER] Short movies after 2015 ===")
    save_dataframe(result, "outputs/arsen_b/filter_recent_short_movies")


def filter_documentaries(df: DataFrame):
    """
    Filter all titles that belong to the Documentary genre.
    """
    result = df.filter(
        (col("genres").isNotNull()) &
        (col("genres").contains("Documentary"))
    )
    print("\n=== [FILTER] Genre: Documentary ===")
    save_dataframe(result, "outputs/arsen_b/filter_documentaries")


# === JOINS ===

def join_movies_with_high_votes(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Join titles and ratings to find movies with more than 50,000 votes.
    """
    result = df_titles.join(df_ratings, on="tconst") \
        .filter(col("numVotes") > 50000)
    print("\n=== [JOIN] Movies with >50k votes ===")
    save_dataframe(result, "outputs/arsen_b/join_high_votes")


def join_top_rated_series(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Join titles and ratings to find top-rated TV series.
    """
    result = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "tvSeries") & (col("averageRating") >= 8.5))
    print("\n=== [JOIN] Top-rated TV series (rating >= 8.5) ===")
    save_dataframe(result, "outputs/arsen_b/join_top_rated_series")


# === GROUP BY ===

def group_by_type_avg_rating(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Average rating per title type (movie, short, tvSeries, etc.).
    """
    joined = df_titles.join(df_ratings, on="tconst")
    result = joined.groupBy("titleType").avg("averageRating").orderBy("avg(averageRating)", ascending=False)
    print("\n=== [GROUP BY] Avg rating per title type ===")
    save_dataframe(result, "outputs/arsen_b/group_by_type_avg_rating")


def group_by_decade_count(df: DataFrame):
    """
    Count number of titles per decade.
    """
    result = df.filter(col("startYear").isNotNull()) \
        .withColumn("decade", (col("startYear") / 10).cast("int") * 10) \
        .groupBy("decade").count().orderBy("decade")
    print("\n=== [GROUP BY] Count of titles per decade ===")
    save_dataframe(result, "outputs/arsen_b/group_by_decade_count")


# === WINDOW FUNCTIONS ===

def top5_movies_per_genre(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Top-5 rated movies per genre.
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "movie") & col("genres").isNotNull())

    window = Window.partitionBy("genres").orderBy(col("averageRating").desc())
    result = joined.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5)
    print("\n=== [WINDOW] Top-5 movies per genre ===")
    save_dataframe(result, "outputs/arsen_b/window_top5_movies_per_genre")


def most_voted_movie_per_genre(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Most voted movie per genre.
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "movie") & col("genres").isNotNull())

    window = Window.partitionBy("genres").orderBy(col("numVotes").desc())
    result = joined.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1)
    print("\n=== [WINDOW] Most voted movie per genre ===")
    save_dataframe(result, "outputs/arsen_b/window_most_voted_per_genre")
