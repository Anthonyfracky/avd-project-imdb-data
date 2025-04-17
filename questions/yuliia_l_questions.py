from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count, rank
from pyspark.sql.window import Window
from utils import save_dataframe



# === FILTERS ===

def filter_movies_by_runtime_and_genre(df: DataFrame):
    """
    Movies with runtime between 120 and 180 minutes and genres 'Drama' or 'Thriller'
    """
    result = df.filter(
        (col("runtimeMinutes").cast("int").between(120, 180)) &
        ((col("genres").like("%Drama%")) | (col("genres").like("%Thriller%")))
    )
    print("\n=== [FILTER] Movies with runtime 120-180 min and genres Drama/Thriller ===")
    save_dataframe(result, "outputs/yuliia_l_questions/filter_runtime_genre")


def filter_recent_popular_movies(basics: DataFrame, ratings: DataFrame):
    """
    Movies released after 2010 with more than 100,000 votes and not marked as adult
    """
    joined_df = basics.join(ratings, on="tconst")

    result = joined_df.filter(
        (col("startYear").cast("int") > 2010) &
        (col("numVotes").cast("int") > 100000) &
        (col("isAdult") == "0")
    )

    print("\n=== [FILTER] Recent popular non-adult movies ===")
    save_dataframe(result, "outputs/yuliia_l_questions/filter_recent_popular")

def filter_episodes_with_title(episodes: DataFrame, basics: DataFrame):
    """
    Filter episodes with parent titles containing the word 'War' released between 1990 and 2000
    """
    joined_df = episodes.join(basics, episodes["parentTconst"] == basics["tconst"])

    result = joined_df.filter(
        (col("primaryTitle").like("%War%")) &  # Filter titles containing 'War'
        (col("startYear").cast("int").between(1990, 2000))  # Filter by release year
    )

    print("\n=== [FILTER] Episodes with 'War' in parent title (1990-2000) ===")
    save_dataframe(result, "outputs/yuliia_l_questions/filter_episodes_with_title")

# === JOINS ===


def join_high_votes_with_akas(ratings: DataFrame, akas: DataFrame):
    """
    Join ratings with akas to find titles with more than 100,000 votes
    """
    joined_df = ratings.join(akas, ratings["tconst"] == akas["titleId"])
    result = joined_df.filter(col("numVotes").cast("int") > 100000)
    print("\n=== [JOIN] Titles with more than 100,000 votes ===")
    save_dataframe(result, "outputs/yuliia_l_questions/join_high_votes_with_akas")


def join_directors_with_crew(crew: DataFrame, name_basics: DataFrame, director_name: str):
    """
    Join crew with name_basics to find movies directed by a specific director
    """
    joined_df = crew.join(name_basics, crew["directors"] == name_basics["nconst"])
    result = joined_df.filter(col("primaryName").like(f"%{director_name}%"))
    print(f"\n=== [JOIN] Movies directed by {director_name} ===")
    save_dataframe(result, "outputs/yuliia_l_questions/join_directors_with_crew")


# === GROUP BY QUESTIONS ===

def group_by_region_language_count(akas: DataFrame):
    """
    Group titles by region and language, and count the number of titles
    """
    result = akas.groupBy("region", "language").agg(count("*").alias("title_count"))
    print("\n=== [GROUP BY] Title count by region and language ===")
    save_dataframe(result, "outputs/yuliia_l_questions/group_by_region_language_count")


def group_by_job_category_count(principals: DataFrame):
    """
    Group principals by job category and count the number of entries
    """
    result = principals.groupBy("category").agg(count("*").alias("entry_count"))
    print("\n=== [GROUP BY] Entry count by job category ===")
    save_dataframe(result, "outputs/yuliia_l_questions/group_by_job_category_count")


# === WINDOW FUNCTION QUESTIONS ===

def rank_actors_by_episode_count(principals: DataFrame, episodes: DataFrame):
    """
    Rank actors by the number of episodes they have participated in
    """
    # Select only necessary columns
    principals_filtered = principals.select("tconst", "nconst", "category").filter(col("category") == "actor")
    episodes_filtered = episodes.select("tconst", "episodeNumber")

    joined_df = principals_filtered.join(episodes_filtered, on="tconst")
    episode_counts = joined_df.groupBy("nconst", "category").agg(count("episodeNumber").alias("episodeCount"))

    window_spec = Window.partitionBy("category").orderBy(col("episodeCount").desc())
    result = episode_counts.withColumn("rank", rank().over(window_spec))

    result.persist()
    save_dataframe(result, "outputs/yuliia_l_questions/rank_actors_by_episode_count")


def top_titles_by_votes_per_region(akas: DataFrame, ratings: DataFrame):
    """
    Find the top 3 titles with the highest votes for each region
    """
    akas_filtered = akas.select("titleId", "region").filter(col("region").isNotNull())
    ratings_filtered = ratings.select("tconst", "numVotes")

    joined_df = akas_filtered.join(ratings_filtered, akas_filtered["titleId"] == ratings_filtered["tconst"])
    window_spec = Window.partitionBy("region").orderBy(col("numVotes").desc())
    result = joined_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") <= 3)

    result.persist()
    save_dataframe(result, "outputs/yuliia_l_questions/top_titles_by_votes_per_region")