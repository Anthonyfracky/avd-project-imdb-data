from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, rank, avg, count, explode, split, countDistinct
from utils import save_dataframe


# === FILTERS ===

def filter_2020_movies(df: DataFrame):
    """
    Movies released exactly in 2020
    """
    result = df.filter(
        (col("titleType") == "movie") &
        (col("startYear") == 2020) &
        (col("startYear").isNotNull())
    )
    print("\n=== [FILTER] 2020 Movies ===")
    save_dataframe(result, "outputs/andrian_v/filter_2020_movies")


def filter_animated_movies(df: DataFrame):
    """
    Filter all animated movies (based on genre)
    """
    result = df.filter(
        (col("titleType") == "movie") &
        (col("genres").contains("Animation"))
    )
    print("\n=== [FILTER] Animated Movies ===")
    save_dataframe(result, "outputs/andrian_v/filter_animated_movies")


def filter_french_titles(df: DataFrame):
    """
 Titles with French language versions
    """
    result = df.filter(
        (col("language") == "fr") &
        (col("title").isNotNull())
    )
    print("\n=== [FILTER] French Titles ===")
    save_dataframe(result, "outputs/andrian_v/filter_french_titles")


def filter_multi_season_series(df: DataFrame):
    """
 TV Series with more than 5 seasons
    """
    result = df.filter(
        (col("titleType") == "tvSeries") &
        (col("endYear").isNotNull()) &
        (col("endYear") - col("startYear") >= 5)
    )
    print("\n=== [FILTER] Series with 5+ Seasons ===")
    save_dataframe(result, "outputs/andrian_v/filter_multi_season_series")


# === JOINS ===


def join_titles_with_multiple_languages(akas: DataFrame, basics: DataFrame):
    """
    Titles available in 3+ languages
    """
    lang_count = akas.groupBy("titleId").agg(countDistinct("language").alias("langs"))
    result = basics.join(lang_count, basics["tconst"] == lang_count["titleId"]) \
                   .filter(col("langs") >= 3)
    print("\n=== [JOIN] Multi-Language Titles ===")
    save_dataframe(result, "outputs/andrian_v/join_multi_lang_titles")


def join_principals_top_movies(df_titles: DataFrame, df_ratings: DataFrame, df_principals: DataFrame):
    """
    Actors in movies with rating >= 9.0
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .join(df_principals, on="tconst") \
        .filter((col("category") == "actor") & (col("averageRating") >= 9.0))

    print("\n=== [JOIN] Actors in movies with rating >= 9.0 ===")
    save_dataframe(joined, "outputs/andrian_v/join_principals_top_movies")

# === GROUP BY ===


def group_by_genre_title_type_count(df: DataFrame):
    """
    Count of title types per genre.
    """
    result = df.filter((col("genres").isNotNull()) & (col("titleType").isNotNull())) \
        .groupBy("genres", "titleType") \
        .count() \
        .orderBy("genres", "count", ascending=False)

    print("\n=== [GROUP BY] Count of title types per genre ===")
    save_dataframe(result, "outputs/andrian_v/group_by_genre_title_type_count")


def group_by_decade_avg_rating(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Average movie rating per decade.
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "movie") & col("startYear").isNotNull())

    result = joined.withColumn("decade", (col("startYear") / 10).cast("int") * 10) \
        .groupBy("decade").avg("averageRating") \
        .orderBy("decade")

    print("\n=== [GROUP BY] Avg movie rating per decade ===")
    save_dataframe(result, "outputs/andrian_v/group_by_decade_avg_rating")

# === WINDOW FUNCTIONS ===


def rank_actors_by_movie_count(principals: DataFrame):
    """
    Rank actors by how many movies they played in
    """
    actors_df = principals.filter(col("category") == "actor")

    grouped = actors_df.groupBy("nconst").agg(count("*").alias("movie_count"))

    window = Window.orderBy(col("movie_count").desc())

    ranked = grouped.withColumn("rank", row_number().over(window))

    print("\n=== [WINDOW] Actors Ranked By Movie Count ===")
    save_dataframe(ranked, "outputs/andrian_v/rank_actors_by_movie_count")


def top_titles_by_votes_per_year(basics: DataFrame, ratings: DataFrame):
    """
    For each year, find top 3 movies by numVotes
    """
    joined = basics.join(ratings, "tconst") \
        .filter((col("titleType") == "movie") & (col("startYear").isNotNull()))

    window = Window.partitionBy("startYear").orderBy(col("numVotes").desc())

    result = joined.withColumn("rank", rank().over(window)) \
        .filter(col("rank") <= 3)

    print("\n=== [WINDOW] Top 3 Most Voted Movies Per Year ===")
    save_dataframe(result, "outputs/andrian_v/top_titles_by_votes_per_year")