from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count, rank, dense_rank
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


def filter_group_by_region_episodes(episodes: DataFrame, akas: DataFrame, ratings: DataFrame, basics: DataFrame):
    """
    Episodes released between 2000 and 2020, joined with basics, akas, and ratings, grouped by region
    """
    episodes_with_year = episodes.join(basics, episodes["tconst"] == basics["tconst"]) \
                                  .select(episodes["tconst"].alias("episodes_tconst"),
                                          "parentTconst", "seasonNumber", "episodeNumber", "startYear")

    episodes_filtered = episodes_with_year.filter(
        col("seasonNumber").cast("int").isNotNull() &
        col("episodeNumber").cast("int").isNotNull()
    ).filter(
        col("startYear").cast("int").between(2000, 2020)
    )
    episodes_with_akas = episodes_filtered.join(akas, episodes_filtered["parentTconst"] == akas["titleId"]) \
                                          .select("episodes_tconst", "region", "titleId")
    joined_df = episodes_with_akas.join(ratings, episodes_with_akas["episodes_tconst"] == ratings["tconst"]) \
                                  .select("region", "episodes_tconst")
    result = joined_df.groupBy("region").agg(count("*").alias("episode_count"))
    print("\n=== [FILTER] Episodes by region (2000-2020) ===")
    save_dataframe(result, "outputs/yuliia_l_questions/filter_group_by_region_episodes")

def filter_episodes_with_title(episodes: DataFrame, basics: DataFrame):
    """
    Filter episodes with parent titles containing the word 'War' released between 1990 and 2000
    """
    joined_df = episodes.join(basics, episodes["parentTconst"] == basics["tconst"])

    result = joined_df.filter(
        (col("primaryTitle").like("%War%")) &
        (col("startYear").cast("int").between(1990, 2000))
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


# === GROUP BY ===

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


# === WINDOW FUNCTION ===

def rank_actors_by_episode_count(principals: DataFrame, episodes: DataFrame):
    """
    Rank actors by the number of episodes they have participated in
    """
    principals_filtered = principals.select("tconst", "nconst", "category").filter(col("category") == "actor")
    episodes_filtered = episodes.select("tconst", "episodeNumber")

    joined_df = principals_filtered.join(episodes_filtered, on="tconst")
    episode_counts = joined_df.groupBy("nconst", "category").agg(count("episodeNumber").alias("episodeCount"))

    window_spec = Window.partitionBy("category").orderBy(col("episodeCount").desc())
    result = episode_counts.withColumn("rank", rank().over(window_spec))

    result.persist()
    save_dataframe(result, "outputs/yuliia_l_questions/rank_actors_by_episode_count")


def top_titles_by_votes_per_region_dense_rank(akas: DataFrame, ratings: DataFrame):
    """
    Find the top 3 titles with the highest votes for each region using dense_rank
    """
    akas_filtered = akas.select("titleId", "region").filter(col("region").isNotNull())
    ratings_filtered = ratings.select("tconst", "numVotes")

    joined_df = akas_filtered.join(ratings_filtered, akas_filtered["titleId"] == ratings_filtered["tconst"])

    window_spec = Window.partitionBy("region").orderBy(col("numVotes").desc())
    result = joined_df.withColumn("dense_rank", dense_rank().over(window_spec)).filter(col("dense_rank") <= 3)

    result.persist()
    save_dataframe(result, "outputs/yuliia_l_questions/top_titles_by_votes_per_region_dense_rank")