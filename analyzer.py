from reader import read_title_basics, read_title_ratings


def analyze_df(df, title, columns=None):
    if columns is None:
        columns = []
    print(f"=== [{title}] SCHEMA ===")
    df.printSchema()

    print(f"=== [{title}] COUNTS ===")
    print(f"Total rows: {df.count()}")
    print(f"Total columns: {len(df.columns)}")

    if columns:
        print(f"=== [{title}] STATISTICS ===")
        df.select(columns).describe().show()
