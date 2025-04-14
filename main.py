import os
from reader import read_title_basics, read_title_ratings, read_title_akas, read_title_principals
import questions.anton_f_questions as q
import questions.arsen_b_questions as q2
import questions.andrian_v_questions as q3
from utils import configure_spark_session
from analyzer import analyze_basics, analyze_ratings


def main():
    os.makedirs("outputs/anton_f", exist_ok=True)
    os.makedirs("outputs/arsen_b", exist_ok=True)
    os.makedirs("outputs/andrian_v", exist_ok=True)

    spark = configure_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    basics = read_title_basics(spark)
    ratings = read_title_ratings(spark)
    akas = read_title_akas(spark)
    principals = read_title_principals(spark)

    print(f"Loaded title.basics with {basics.count()} rows")
    print(f"Loaded title.ratings with {ratings.count()} rows")
    print(f"Loaded title.akas with {akas.count()} rows")

    # === DATA ANALYSIS (description + statistics) ===
    analyze_basics(basics)
    analyze_ratings(ratings)

    print("=== ANTON_F'S QUESTIONS ===")

    # === ANTON'S QUESTIONS ===
    q.filter_long_movies(basics)
    q.filter_old_non_adult_movies(basics)
    q.filter_comedy_genre(basics)
    q.join_with_ratings_high_rated(basics, ratings)
    q.join_and_filter_votes(basics, ratings)
    q.group_by_year_count(basics)
    q.group_by_genre_avg_runtime(basics)
    q.top_movies_by_rating_per_year(basics, ratings)
    q.rank_movies_by_votes(basics, ratings)

    print("=== ARSEN'S QUESTIONS ===")

    # === ARSEN'S QUESTIONS ===
    q2.filter_ukrainian_titles(akas)
    q2.filter_recent_short_movies(basics)
    q2.filter_documentaries(basics)
    q2.join_movies_with_high_votes(basics, ratings)
    q2.join_top_rated_series(basics, ratings)
    q2.group_by_type_avg_rating(basics, ratings)
    q2.group_by_decade_count(basics)
    q2.top5_movies_per_genre(basics, ratings)
    q2.most_voted_movie_per_genre(basics, ratings)

    print("=== ANDRIAN'S QUESTIONS ===")
    # === ANDRIAN'S QUESTIONS ===
    q3.filter_2020_movies(basics)
    q3.filter_animated_movies(basics)
    q3.filter_french_titles(akas)
    q3.filter_multi_season_series(basics)
    q3.join_titles_with_multiple_languages(akas, basics)
    q3.join_principals_top_movies(basics, ratings, principals)
    q3.group_by_genre_title_type_count(basics)
    q3.group_by_decade_avg_rating(basics, ratings)
    q3.rank_actors_by_movie_count(principals)
    q3.top_titles_by_votes_per_year(basics, ratings)

    spark.stop()


if __name__ == "__main__":
    main()
