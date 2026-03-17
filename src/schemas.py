from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

TITLE_BASICS_SCHEMA = StructType([
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

TITLE_RATINGS_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True),
])

NAME_BASICS_SCHEMA = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True),
])

TITLE_PRINCIPALS_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True),
])
