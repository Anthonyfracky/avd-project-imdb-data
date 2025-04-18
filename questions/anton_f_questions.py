from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, rank, avg
from utils import save_dataframe


# === COMPLEX QUERY ===

def top_drama_movies_by_region(df_titles: DataFrame, df_ratings: DataFrame, df_akas: DataFrame):
    """
    Drama movies after 2010 with rating > 8.5 and their distribution by region
    """
    filtered = df_titles.filter(
        (col("genres").contains("Drama")) &
        (col("startYear") > 2010) &
        (col("titleType") == "movie")
    )

    joined = filtered.join(df_ratings, on="tconst") \
        .filter(col("averageRating") > 8.5)

    df_akas = df_akas.withColumnRenamed("titleId", "tconst")

    with_region = joined.join(df_akas, on="tconst") \
        .filter(col("region").isNotNull()) \
        .select("primaryTitle", "averageRating", "region")

    print("\n=== [COMPLEX] Top drama movies >8.5 after 2010 with regions ===")
    save_dataframe(with_region, "outputs/anton_f/complex_top_drama_by_region")


# === FILTERS ===


def filter_long_movies(df: DataFrame):
    """
    Movies longer than 2 hours (120 minutes)
    """
    result = df.filter(
        (col("titleType") == "movie") &
        (col("runtimeMinutes").isNotNull()) &
        (col("runtimeMinutes") > 120)
    )
    print("\n=== [FILTER] Long Movies > 120min ===")
    save_dataframe(result, "outputs/anton_f/filter_long_movies")


def filter_old_non_adult_movies(df: DataFrame):
    """
    Non-adult movies before 1950
    """
    result = df.filter(
        (col("isAdult") == 0) &
        (col("startYear").isNotNull()) &
        (col("startYear") < 1950)
    )
    print("\n=== [FILTER] Old Non-Adult Movies < 1950 ===")
    save_dataframe(result, "outputs/anton_f/filter_old_non_adult_movies")


def filter_comedy_genre(df: DataFrame):
    """
    Movies with genre 'Comedy'
    """
    result = df.filter(
        (col("genres").isNotNull()) &
        (col("genres").contains("Comedy"))
    )
    print("\n=== [FILTER] Genre: Comedy ===")
    save_dataframe(result, "outputs/anton_f/filter_comedy_genre")


# === JOINS ===

def join_with_ratings_high_rated(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Movies with rating > 8.0
    """
    result = df_titles.join(df_ratings, on="tconst") \
        .filter(col("averageRating") > 8.0)
    print("\n=== [JOIN] Movies with rating > 8.0 ===")
    save_dataframe(result, "outputs/anton_f/join_high_rated")


def join_and_filter_votes(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Movies with more than 100,000 votes
    """
    result = df_titles.join(df_ratings, on="tconst") \
        .filter(col("numVotes") > 100000)
    print("\n=== [JOIN] Movies with >100k votes ===")
    save_dataframe(result, "outputs/anton_f/join_popular_votes")


# === GROUP BY ===

def group_by_year_count(df: DataFrame):
    """
    Count of movies per year
    """
    result = df.filter(col("startYear").isNotNull()) \
        .groupBy("startYear").count().orderBy("startYear")
    print("\n=== [GROUP BY] Count of titles per year ===")
    save_dataframe(result, "outputs/anton_f/group_by_year_count")


def group_by_genre_avg_runtime(df: DataFrame):
    """
    Average runtime of movies per genre
    """
    result = df.filter(col("runtimeMinutes").isNotNull() & col("genres").isNotNull()) \
        .groupBy("genres") \
        .avg("runtimeMinutes") \
        .orderBy("avg(runtimeMinutes)", ascending=False)
    print("\n=== [GROUP BY] Avg runtime per genre ===")
    save_dataframe(result, "outputs/anton_f/group_by_genre_avg_runtime")


# === WINDOW FUNCTIONS ===

def top_movies_by_rating_per_year(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Top-1 movie by rating per year
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "movie") & col("startYear").isNotNull())

    window = Window.partitionBy("startYear").orderBy(
        col("averageRating").desc())
    result = joined.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1)
    print("\n=== [WINDOW] Top-1 rated movie per year ===")
    save_dataframe(result, "outputs/anton_f/window_top_movie_per_year")


def rank_movies_by_votes(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Top-3 movies by votes per year
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "movie") & col("startYear").isNotNull())

    window = Window.partitionBy("startYear").orderBy(col("numVotes").desc())
    result = joined.withColumn("rank", rank().over(window)) \
        .filter(col("rank") <= 3)
    print("\n=== [WINDOW] Top-3 voted movies per year ===")
    save_dataframe(result, "outputs/anton_f/window_top3_votes_per_year")
