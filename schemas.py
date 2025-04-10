from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

title_basics_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", IntegerType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True),
])

title_ratings_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True),
])
