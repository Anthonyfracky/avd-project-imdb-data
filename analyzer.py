from reader import read_title_basics, read_title_ratings


def analyze_basics(df):
    print("=== [TITLE.BASICS] SCHEMA ===")
    df.printSchema()

    print("=== [TITLE.BASICS] COUNTS ===")
    print(f"Total rows: {df.count()}")
    print(f"Total columns: {len(df.columns)}")

    print("=== [TITLE.BASICS] STATISTICS ===")
    df.select("startYear", "endYear", "runtimeMinutes").describe().show()


def analyze_ratings(df):
    print("=== [TITLE.RATINGS] SCHEMA ===")
    df.printSchema()

    print("=== [TITLE.RATINGS] COUNTS ===")
    print(f"Total rows: {df.count()}")
    print(f"Total columns: {len(df.columns)}")

    print("=== [TITLE.RATINGS] STATISTICS ===")
    df.select("averageRating", "numVotes").describe().show()
