from pathlib import Path
from pyspark.sql import SparkSession

from src.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA
from src.reader import read_tsv
from src.preprocessing import (
    print_overview,
    print_numeric_stats,
    print_missing_values,
    print_duplicate_count,
    print_column_informativeness,
)

project_dir = Path(__file__).resolve().parent
data_dir = project_dir.parent / "data" / "imdb"

title_basics_path = data_dir / "title.basics.tsv.gz"
title_ratings_path = data_dir / "title.ratings.tsv.gz"

spark = SparkSession.builder.appName("imdb_preprocessing_step2").getOrCreate()

title_basics_df = read_tsv(spark, title_basics_path, TITLE_BASICS_SCHEMA)
title_ratings_df = read_tsv(spark, title_ratings_path, TITLE_RATINGS_SCHEMA)

print_overview(title_basics_df, "TITLE_BASICS")
print_numeric_stats(title_basics_df, "TITLE_BASICS")
print_missing_values(title_basics_df, "TITLE_BASICS")
print_duplicate_count(title_basics_df, "TITLE_BASICS", ["tconst"])
print_column_informativeness(title_basics_df, "TITLE_BASICS")

print_overview(title_ratings_df, "TITLE_RATINGS")
print_numeric_stats(title_ratings_df, "TITLE_RATINGS")
print_missing_values(title_ratings_df, "TITLE_RATINGS")
print_duplicate_count(title_ratings_df, "TITLE_RATINGS", ["tconst"])
print_column_informativeness(title_ratings_df, "TITLE_RATINGS")

spark.stop()
