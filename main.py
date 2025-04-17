import os
from reader import read_title_basics, read_title_ratings, read_title_akas, read_title_principals, read_title_crew, \
    read_name_basics, read_title_episode
import questions.anton_f_questions as q
import questions.arsen_b_questions as q2
import questions.andrian_v_questions as q3
import questions.anton_l_questions as q4
import questions.yuliia_l_questions as q5
from utils import configure_spark_session
from analyzer import analyze_df


def main():
    os.makedirs("outputs/   anton_f", exist_ok=True)
    os.makedirs("outputs/arsen_b", exist_ok=True)
    os.makedirs("outputs/andrian_v", exist_ok=True)
    os.makedirs("outputs/yuliia_l", exist_ok=True)

    spark = configure_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    basics = read_title_basics(spark)
    ratings = read_title_ratings(spark)
    akas = read_title_akas(spark)
    principals = read_title_principals(spark)
    crew = read_title_crew(spark)
    name_basics = read_name_basics(spark)
    episodes = read_title_episode(spark)

    # === DATA ANALYSIS (description + statistics) ===
    print("\n=== DATA ANALYSIS without cleaning ===")
    print("\n")
    analyze_df(basics, "Basics", ["startYear", "endYear", "runtimeMinutes"])
    analyze_df(ratings, "Ratings", ["averageRating", "numVotes"])
    analyze_df(akas, "Akas", ["region", "language"])
    analyze_df(principals, "Principals", ["category", "job"])
    analyze_df(crew, "Crew", ["directors", "writers"])
    analyze_df(name_basics, "Name_basics", ["birthYear", "primaryProfession"])
    analyze_df(episodes, "Episode", ["tconst", "parentTconst"])

    # === CLEANING ===

    # 1. title.basics: remove rows with null genres, filter isAdult ∉ {0,1}
    basics = basics.filter(basics.genres.isNotNull()) \
                   .filter((basics.isAdult == 0) | (basics.isAdult == 1))

    # 2. title.crew: remove rows where both directors and writers are null
    crew = crew.filter(crew.directors.isNotNull() | crew.writers.isNotNull())

    # 3. title.episode: drop rows with nulls, remove strange entries (tconst or parentTconst should be valid)
    title_episode = episodes.dropna(subset=["tconst", "parentTconst", "seasonNumber", "episodeNumber"])

    # 4. title.akas: drop rows with region = \N
    akas = akas.filter(akas.region.isNotNull())

    # 5. name.basics: drop rows with primaryProfession = \N
    name_basics = name_basics.filter(name_basics.primaryProfession.isNotNull())

    print("\n")
    print("\n=== DATA ANALYSIS with cleaning ===")

    # === DATA ANALYSIS (description + statistics) ===
    analyze_df(basics, "Basics", ["startYear", "endYear", "runtimeMinutes"])
    analyze_df(ratings, "Ratings", ["averageRating", "numVotes"])
    analyze_df(akas, "Akas", ["region", "language"])
    analyze_df(principals, "Principals", ["category", "job"])
    analyze_df(crew, "Crew", ["directors", "writers"])
    analyze_df(name_basics, "Name_basics", ["birthYear", "primaryProfession"])
    analyze_df(title_episode, "Title_episode", ["tconst", "parentTconst"])

    print("=== ANTON_F'S QUESTIONS ===")

    #  === ANTON'S QUESTIONS ===
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

    #  === ARSEN'S QUESTIONS ===
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

    #  === ANDRIAN'S QUESTIONS ===
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

    print("=== ANTON_L'S QUESTIONS ===")

    #  === ANTON_L'S QUESTIONS ===
    q4.top_directors_by_high_rated_movies(basics, ratings, crew, name_basics)
    q4.directors_also_writers(basics, crew)
    q4.director_average_ratings(basics, ratings, crew, name_basics)
    q4.top_directors_by_decade(basics, ratings, crew, name_basics)
    q4.writers_with_multiple_directors(crew, name_basics)
    q4.compare_dual_role_vs_separate(basics, ratings, crew)
    q4.top_genre_by_director(basics, crew, name_basics)

    print("=== YULIIA'S QUESTIONS ===")

    # print("=== YULIIA_L'S QUESTIONS ===")
    q5.filter_movies_by_runtime_and_genre(basics)
    q5.filter_group_by_region_episodes(episodes, akas, ratings, basics)
    q5.filter_episodes_with_title(episodes, basics)
    q5.join_directors_with_crew(crew, name_basics, "Steven Spielberg")
    q5.join_high_votes_with_akas(ratings, akas)
    q5.group_by_region_language_count(akas)
    q5.group_by_job_category_count(principals)
    q5.rank_actors_by_episode_count(principals, episodes)
    q5.top_titles_by_votes_per_region_dense_rank(akas, ratings)

    spark.stop()


if __name__ == "__main__":
    main()