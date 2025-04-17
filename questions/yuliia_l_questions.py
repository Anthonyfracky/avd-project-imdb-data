from pyspark.sql import DataFrame
from pyspark.sql.functions import col
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
    # Join basics with ratings to include numVotes
    joined_df = basics.join(ratings, on="tconst")

    # Apply the filter
    result = joined_df.filter(
        (col("startYear").cast("int") > 2010) &
        (col("numVotes").cast("int") > 100000) &
        (col("isAdult") == "0")
    )

    print("\n=== [FILTER] Recent popular non-adult movies ===")
    save_dataframe(result, "outputs/yuliia_l_questions/filter_recent_popular")

def filter_movies_with_title(df: DataFrame):
    """
    Movies with titles containing the word 'War' released between 1990 and 2000
    """
    result = df.filter(
        (col("primaryTitle").like("%War%")) &
        (col("startYear").cast("int").between(1990, 2000))
    )
    print("\n=== [FILTER] Movies with 'War' in title (1990-2000) ===")
    save_dataframe(result, "outputs/yuliia_l_questions/filter_title_war")