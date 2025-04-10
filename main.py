import os
from reader import read_title_basics, read_title_ratings
import questions.anton_f_questions as q
from utils import configure_spark_session
from analyzer import analyze_basics, analyze_ratings


def main():
    os.makedirs("outputs/anton_f", exist_ok=True)

    spark = configure_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    basics = read_title_basics(spark)
    ratings = read_title_ratings(spark)

    print(f"Loaded title.basics with {basics.count()} rows")
    print(f"Loaded title.ratings with {ratings.count()} rows")

    # === DATA ANALYSIS (description + statistics) ===
    analyze_basics(basics)
    analyze_ratings(ratings)

    # === FILTERS ===
    q.filter_long_movies(basics)
    q.filter_old_non_adult_movies(basics)
    q.filter_comedy_genre(basics)

    # === JOINS ===
    q.join_with_ratings_high_rated(basics, ratings)
    q.join_and_filter_votes(basics, ratings)

    # === GROUP BY ===
    q.group_by_year_count(basics)
    q.group_by_genre_avg_runtime(basics)

    # === WINDOW FUNCTIONS ===
    q.top_movies_by_rating_per_year(basics, ratings)
    q.rank_movies_by_votes(basics, ratings)

    spark.stop()


if __name__ == "__main__":
    main()
