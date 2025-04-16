from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, explode, count, avg, desc, floor, lit, array_intersect
from utils import save_dataframe



def top_directors_by_high_rated_movies(basics: DataFrame, ratings: DataFrame, crew: DataFrame, name_basics: DataFrame):
    result = basics.join(ratings, "tconst") \
        .filter((col("titleType") == "movie") & (col("averageRating") > 8.0)) \
        .join(crew, "tconst") \
        .withColumn("director", explode(split("directors", ","))) \
        .groupBy("director").count() \
        .join(name_basics, name_basics.nconst == col("director")) \
        .select("director", "primaryName", "count") \
        .orderBy(desc("count")).limit(10)
    save_dataframe(result, "outputs/anton_l/top_directors_high_rated")
    return result

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, explode, count, desc
from utils import save_dataframe

def directors_also_writers(basics: DataFrame, crew: DataFrame):
    directors = crew.select("tconst", explode(split(col("directors"), ",")).alias("director"))
    writers = crew.select("tconst", explode(split(col("writers"), ",")).alias("writer"))
    dual_role = directors.join(writers, "tconst") \
                         .filter(col("director") == col("writer")) \
                         .select("tconst", col("director").alias("person"))
    result = dual_role.groupBy("person") \
                      .agg(count("tconst").alias("movie_count")) \
                      .orderBy(desc("movie_count"))
    save_dataframe(result, "outputs/anton_l/director_also_writer_movies_count")
    return result

def director_average_ratings(basics: DataFrame, ratings: DataFrame, crew: DataFrame, name_basics: DataFrame):
    result = basics.filter(col("titleType") == "movie") \
        .join(ratings, "tconst") \
        .join(crew, "tconst") \
        .withColumn("director", explode(split("directors", ","))) \
        .groupBy("director").agg(count("tconst").alias("movieCount"), avg("averageRating").alias("avgRating")) \
        .filter(col("movieCount") >= 5) \
        .join(name_basics, name_basics.nconst == col("director")) \
        .select("director", "primaryName", "movieCount", "avgRating") \
        .orderBy(desc("avgRating"))
    save_dataframe(result, "outputs/anton_l/director_avg_ratings")
    return result

def top_directors_by_decade(basics: DataFrame, ratings: DataFrame, crew: DataFrame, name_basics: DataFrame):
    from pyspark.sql.functions import floor, col, avg, count, explode, split, desc, rank
    from pyspark.sql.window import Window

    movies = basics.filter((col("titleType") == "movie") & col("startYear").isNotNull()) \
                   .withColumn("decade", floor(col("startYear") / 10) * 10)
    movies = movies.join(ratings, "tconst").join(crew, "tconst")
    movies = movies.withColumn("director", explode(split(col("directors"), ",")))
    director_stats = movies.groupBy("decade", "director") \
                           .agg(avg("averageRating").alias("avgRating"),
                                count("tconst").alias("movieCount")) \
                           .filter(col("movieCount") >= 3)
    director_stats = director_stats.join(name_basics, name_basics.nconst == col("director")) \
                                   .select("decade", "director", "primaryName", "avgRating")

    window = Window.partitionBy("decade").orderBy(desc("avgRating"))
    ranked = director_stats.withColumn("rank", rank().over(window))

    result = ranked.filter(col("rank") == 1).orderBy("decade")

    save_dataframe(result, "outputs/anton_l/top_director_by_decade")
    return result


def writers_with_multiple_directors(crew: DataFrame, name_basics: DataFrame):
    result = crew.filter(col("writers").isNotNull() & col("directors").isNotNull()) \
        .withColumn("writer", explode(split("writers", ","))) \
        .withColumn("director", explode(split("directors", ","))) \
        .groupBy("writer").agg(count("director").alias("director_count")) \
        .filter(col("director_count") > 1) \
        .join(name_basics, name_basics.nconst == col("writer")) \
        .select("writer", "primaryName", "director_count") \
        .orderBy(desc("director_count"))
    save_dataframe(result, "outputs/anton_l/writer_director_collaborations")
    return result

def compare_dual_role_vs_separate(basics: DataFrame, ratings: DataFrame, crew: DataFrame):
    from pyspark.sql.functions import split, array_intersect, size, col, lit, when, avg, count

    crew_filtered = crew.filter(col("directors").isNotNull() & col("writers").isNotNull()) \
        .withColumn("is_dual_role", size(array_intersect(split(col("directors"), ","), split(col("writers"), ","))) > 0)

    movies = basics.filter(col("titleType") == "movie") \
        .join(crew_filtered.select("tconst", "is_dual_role"), on="tconst") \
        .join(ratings, on="tconst")

    summary = movies.groupBy("is_dual_role").agg(
        count("tconst").alias("movie_count"),
        avg("averageRating").alias("avg_rating"),
        avg("numVotes").alias("avg_votes")
    )

    readable = summary.withColumn("role_type", when(col("is_dual_role"), lit("Director = Writer"))
                                              .otherwise(lit("Director ≠ Writer"))) \
                      .select("role_type", "movie_count", "avg_rating", "avg_votes") \
                      .orderBy("role_type")

    save_dataframe(readable, "outputs/anton_l/dual_role_vs_separate")
    return readable


def top_genre_by_director(basics: DataFrame, crew: DataFrame, name_basics: DataFrame):
    result = basics.filter(col("genres").isNotNull()) \
        .withColumn("genre", explode(split("genres", ","))) \
        .join(crew, "tconst") \
        .withColumn("director", explode(split("directors", ","))) \
        .groupBy("director", "genre").count() \
        .filter(col("count") >= 3) \
        .join(name_basics, name_basics.nconst == col("director")) \
        .select("director", "primaryName", "genre", "count") \
        .orderBy(desc("count"))
    save_dataframe(result, "outputs/anton_l/director_top_genres")
    return result
