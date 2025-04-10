# Project Structure and Dataset Setup

## Directory Overview

```
imdb-spark-project/
│
├── imdb-data/                        # <== ⚠️ Must be created manually. Place IMDb dataset files here
│   ├── title.basics.tsv.gz
│   └── title.ratings.tsv.gz
│
├── outputs/                          # <== ⚠️ Must be created manually. Contains CSV results of business queries for users. Create your own folder
│   └── user/                         # Example
│
├── questions/
│   └── anton_f_questions.py          # anton_f's queries
│
├── schemas.py                        # StructType definitions for all datasets
├── reader.py                         # Functions to load TSV data using schemas
├── analyzer.py                       # Analysis functions
├── utils.py                          # SparkSession configuration utility
├── main.py                           # Main application entry point
├── requirements.txt                  # Python dependencies
├── Dockerfile                        # Docker environment definition
└── .gitignore                        # Ignore data/outputs to keep repo clean
```

---

## `imdb-data/` Folder

⚠️ This folder **must be created manually** — it is **not included in the repository** because it contains large raw data.

Download IMDb datasets from the official source:
[https://datasets.imdbws.com/](https://datasets.imdbws.com/)

---
