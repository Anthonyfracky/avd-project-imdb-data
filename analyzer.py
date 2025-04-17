import os

import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("TkAgg")  # Use TkAgg backend for matplotlib
import seaborn as sns


def analyze_df(df, title, columns=None, sample_rows=5, plot=True, limit_for_plot=10000):
    if columns is None:
        columns = []

    print(f"\n=== [{title}] SCHEMA ===")
    df.printSchema()

    print(f"\n=== [{title}] COUNTS ===")
    row_count = df.count()
    print(f"Total rows: {row_count}")
    print(f"Total columns: {len(df.columns)}")

    print(f"\n=== [{title}] SAMPLE ROWS ===")
    df.show(sample_rows)

    print(f"\n=== [{title}] NULL VALUES PER COLUMN ===")
    for col in df.columns:
        null_count = df.filter(df[col].isNull() | (df[col] == "")).count()
        print(f"{col}: {null_count} nulls")

    if columns:
        print(f"\n=== [{title}] DESCRIPTIVE STATISTICS ===")
        df.select(columns).describe().show()

        print(f"\n=== [{title}] APPROXIMATE QUANTILES ===")
        for col in columns:
            if str(df.schema[col].dataType) in ["IntegerType", "DoubleType", "LongType"]:
                quantiles = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.01)
                print(f"{col} (Q1, Median, Q3): {quantiles}")

        print(f"\n=== [{title}] TOP UNIQUE VALUES ===")
        for col in columns:
            if str(df.schema[col].dataType) == "StringType":
                df.groupBy(col).count().orderBy("count", ascending=False).show(10)

        # --- Гістограми ---
        if plot:
            try:
                pdf = df.select(columns).dropna().limit(limit_for_plot).toPandas()

                numeric_cols = pdf.select_dtypes(include=["int64", "float64"]).columns
                for col in numeric_cols:
                    plt.figure(figsize=(8, 4))
                    sns.histplot(pdf[col], bins=30, kde=True)
                    plt.title(f"{title} - Розподіл значень {col}")
                    plt.xlabel(col)
                    plt.ylabel("Частота")
                    plt.tight_layout()

                    # Save the plot
                    plot_path = f"outputs/{title}/{col}_histogram.png"
                    os.makedirs(os.path.dirname(plot_path), exist_ok=True)
                    plt.savefig(plot_path)
                    plt.close()
                    print(f"[{title}] Гістограма {col} збережена: {plot_path}")
            except Exception as e:
                print(f"[{title}] Неможливо побудувати графіки: {e}")
