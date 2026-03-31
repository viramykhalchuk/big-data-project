from pathlib import Path
from pyspark.sql import SparkSession

from src.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA
from src.reader import read_tsv
from src.cleaning import clean_title_basics, clean_title_ratings
from src.preprocessing import (
    print_overview,
    print_missing_values,
    print_duplicate_count,
)

project_dir = Path(__file__).resolve().parent
data_dir = project_dir.parent / "data" / "imdb"

title_basics_path = data_dir / "title.basics.tsv.gz"
title_ratings_path = data_dir / "title.ratings.tsv.gz"

spark = SparkSession.builder.appName("imdb_cleaning_step").getOrCreate()

title_basics_raw = read_tsv(spark, title_basics_path, TITLE_BASICS_SCHEMA)
title_ratings_raw = read_tsv(spark, title_ratings_path, TITLE_RATINGS_SCHEMA)

title_basics_clean = clean_title_basics(title_basics_raw)
title_ratings_clean = clean_title_ratings(title_ratings_raw)

print("AFTER CLEANING: TITLE_BASICS")
print_overview(title_basics_clean, "TITLE_BASICS_CLEAN")
print_missing_values(title_basics_clean, "TITLE_BASICS_CLEAN")
print_duplicate_count(title_basics_clean, "TITLE_BASICS_CLEAN", ["tconst"])

print("AFTER CLEANING: TITLE_RATINGS")
print_overview(title_ratings_clean, "TITLE_RATINGS_CLEAN")
print_missing_values(title_ratings_clean, "TITLE_RATINGS_CLEAN")
print_duplicate_count(title_ratings_clean, "TITLE_RATINGS_CLEAN", ["tconst"])

spark.stop()
