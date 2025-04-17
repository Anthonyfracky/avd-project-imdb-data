from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, rank, avg, count, explode, split, countDistinct, round as spark_round
from utils import save_dataframe


# === FILTERS ===

def filter_2020_movies(df: DataFrame):
    """
    Фільми, що вийшли у 2020 році
    """
    result = df.filter(
        (col("titleType") == "movie") &
        (col("startYear") == 2020) &
        (col("startYear").isNotNull())
    ).select("primaryTitle", "startYear", "runtimeMinutes", "genres")

    print("\n=== [FILTER] Фільми, випущені у 2020 році ===")
    # result.show(20, truncate=False)
    save_dataframe(result, "outputs/andrian_v/filter_2020_movies")


def filter_animated_movies(df: DataFrame):
    """
    Анімаційні фільми (за жанром)
    """
    result = df.filter(
        (col("titleType") == "movie") &
        (col("genres").contains("Animation"))
    ).select("primaryTitle", "startYear", "genres", "runtimeMinutes").orderBy(col("startYear").desc())

    print("\n=== [FILTER] Анімаційні фільми ===")
    # result.show(20, truncate=False)
    save_dataframe(result, "outputs/andrian_v/filter_animated_movies")


def filter_french_titles(df: DataFrame):
    """
    Назви, що мають французьку мовну версію
    """
    result = df.filter(
        (col("language") == "fr") &
        (col("title").isNotNull())
    ).select("title", "region", "language", "types")

    print("\n=== [FILTER] Назви з французькою мовною версією ===")
    # result.show(20, truncate=False)
    save_dataframe(result, "outputs/andrian_v/filter_french_titles")


def filter_multi_season_series(df: DataFrame):
    """
    Серіали, що мають понад 5 сезонів
    """
    result = df.filter(
        (col("titleType") == "tvSeries") &
        (col("endYear").isNotNull()) &
        ((col("endYear") - col("startYear")) >= 5)
    ).select("primaryTitle", "startYear", "endYear", (col("endYear") - col("startYear")).alias("num_seasons")).orderBy(col("startYear").desc())

    print("\n=== [FILTER] Серіали з 5 і більше сезонами ===")
    # result.show(20, truncate=False)
    save_dataframe(result, "outputs/andrian_v/filter_multi_season_series")


# === JOINS ===


def join_titles_with_multiple_languages(akas: DataFrame, basics: DataFrame):
    """
    Titles available in 3+ languages (з базовою інформацією).
    """
    lang_count = akas.filter(col("language").isNotNull()) \
                     .groupBy("titleId") \
                     .agg(countDistinct("language").alias("langs")) \
                     .filter(col("langs") >= 3)

    lang_count.cache()

    result = basics.join(lang_count, basics["tconst"] == lang_count["titleId"], "inner") \
                   .select("tconst", "primaryTitle", "titleType", "langs") \
                   .orderBy(col("langs").desc(), col("primaryTitle"))

    print("\n=== [JOIN] Multi-Language Titles (3+ мов) ===")
    save_dataframe(result, "outputs/andrian_v/join_multi_lang_titles")


def join_principals_top_movies(
    df_titles: DataFrame,
    df_ratings: DataFrame,
    df_principals: DataFrame,
    df_names: DataFrame
):
    """
    Актори, які знімались у фільмах з рейтингом >= 9.0
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .join(df_principals, on="tconst") \
        .filter((col("category") == "actor") & (col("averageRating") >= 9.0)) \
        .select("primaryTitle", "averageRating", "nconst")

    final = joined.join(df_names.select("nconst", "primaryName"), on="nconst") \
        .select("primaryTitle", "averageRating", "primaryName")

    print("\n=== [JOIN] Актори, що знімались у фільмах з рейтингом 9.0 і вище ===")
    # final.show(20, truncate=False)
    save_dataframe(final, "outputs/andrian_v/join_principals_top_movies")

# === GROUP BY ===


def group_by_genre_title_type_count(df: DataFrame):
    """
    Count of title types per genre (один жанр — один рядок).
    """
    result = df.filter((col("genres").isNotNull()) & (col("titleType").isNotNull())) \
               .withColumn("genre", explode(split(col("genres"), ","))) \
               .groupBy("genre", "titleType") \
               .count() \
               .orderBy("genre", col("count").desc())

    print("\n=== [GROUP BY] Кількість типів тайтлів за жанрами ===")
    save_dataframe(result, "outputs/andrian_v/group_by_genre_title_type_count")


def group_by_decade_avg_rating(df_titles: DataFrame, df_ratings: DataFrame):
    """
    Середній рейтинг фільмів за десятиліттями (лише десятиліття з >= 50 фільмами)
    """
    joined = df_titles.join(df_ratings, on="tconst") \
        .filter((col("titleType") == "movie") & col("startYear").isNotNull())

    result = joined.withColumn("startYearInt", col("startYear").cast("int")) \
        .withColumn("decade", (col("startYearInt") / 10).cast("int") * 10) \
        .groupBy("decade") \
        .agg(
            count("*").alias("movie_count"),
            spark_round(avg("averageRating"), 2).alias("avg_rating")
        ) \
        .filter(col("movie_count") >= 50) \
        .orderBy("decade")

    print("\n=== [GROUP BY] Середній рейтинг фільмів за десятиліттями (>= 50 фільмів) ===")
    save_dataframe(result, "outputs/andrian_v/group_by_decade_avg_rating_filtered")

# === WINDOW FUNCTIONS ===


def rank_actors_by_movie_count(principals: DataFrame, names: DataFrame):
    """
    Хто з акторів знявся в найбільшій кількості фільмів?
    """
    actors_df = principals.filter(col("category") == "actor")

    grouped = actors_df.groupBy("nconst").agg(count("*").alias("movie_count"))

    # Приєднуємо імена
    with_names = grouped.join(names.select("nconst", "primaryName"), on="nconst")

    window = Window.orderBy(col("movie_count").desc())

    ranked = with_names.withColumn("rank", row_number().over(window)) \
        .select("rank", "primaryName", "movie_count")

    print("\n=== [WINDOW] Actors Ranked By Movie Count ===")
    # ranked.show(10, truncate=False)
    save_dataframe(ranked, "outputs/andrian_v/rank_actors_by_movie_count")


def top_titles_by_votes_per_year(basics: DataFrame, ratings: DataFrame):
    """
    Топ-3 фільми з найбільшою кількістю голосів щороку
    """
    joined = basics.join(ratings, "tconst") \
        .filter((col("titleType") == "movie") & col("startYear").isNotNull())

    window = Window.partitionBy("startYear").orderBy(col("numVotes").desc())

    result = joined.withColumn("rank", rank().over(window)) \
        .filter(col("rank") <= 3) \
        .select(
            "startYear",
            "rank",
            "primaryTitle",
            "numVotes",
            "averageRating"
        ) \
        .orderBy("startYear", "rank")

    print("\n=== [WINDOW] Топ-3 найпопулярніші фільми за кількістю голосів по роках ===")
    # result.show(15, truncate=False)
    save_dataframe(result, "outputs/andrian_v/top_titles_by_votes_per_year")