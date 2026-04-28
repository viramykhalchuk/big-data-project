from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from src.cleaning import clean_title_basics, clean_title_ratings
from src.reader import read_tsv
from src.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA, TITLE_AKAS_SCHEMA
from src.transformations import run_business_questions


project_dir = Path(__file__).resolve().parent
data_dir = project_dir.parent / "data" / "imdb"

title_basics_path = data_dir / "title.basics.tsv.gz"
title_ratings_path = data_dir / "title.ratings.tsv.gz"
title_akas_path = data_dir / "title.akas.tsv.gz"

spark = (
    SparkSession.builder
    .appName("imdb_transformation_step")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\nREADING IMDB DATASETS")

title_basics_raw = read_tsv(spark, title_basics_path, TITLE_BASICS_SCHEMA)
title_ratings_raw = read_tsv(spark, title_ratings_path, TITLE_RATINGS_SCHEMA)
title_akas_raw = read_tsv(spark, title_akas_path, TITLE_AKAS_SCHEMA)

print("\nCLEANING DATASETS")

title_basics_clean = clean_title_basics(title_basics_raw).persist(StorageLevel.MEMORY_AND_DISK)
title_ratings_clean = clean_title_ratings(title_ratings_raw).persist(StorageLevel.MEMORY_AND_DISK)

print("\nMATERIALIZING CLEAN DATASETS")

print(f"title_basics_clean rows: {title_basics_clean.count()}")
print(f"title_ratings_clean rows: {title_ratings_clean.count()}")

print("\nRUNNING BUSINESS QUESTIONS")

output_dir = project_dir / "outputs" / "transformation_results"
run_business_questions(title_basics_clean, title_ratings_clean, title_akas_raw, output_dir)

title_basics_clean.unpersist()
title_ratings_clean.unpersist()

spark.stop()