from pathlib import Path
from pyspark.sql import SparkSession

from src.schemas import (
    TITLE_BASICS_SCHEMA,
    TITLE_RATINGS_SCHEMA,
    NAME_BASICS_SCHEMA,
    TITLE_PRINCIPALS_SCHEMA,
)
from src.reader import read_tsv

project_dir = Path(__file__).resolve().parent
data_dir = project_dir.parent / "data" / "imdb"

title_basics_path = data_dir / "title.basics.tsv.gz"
title_ratings_path = data_dir / "title.ratings.tsv.gz"
name_basics_path = data_dir / "name.basics.tsv.gz"
title_principals_path = data_dir / "title.principals.tsv.gz"

spark = SparkSession.builder.appName("imdb_read_test").getOrCreate()

title_basics_df = read_tsv(spark, title_basics_path, TITLE_BASICS_SCHEMA)
title_ratings_df = read_tsv(spark, title_ratings_path, TITLE_RATINGS_SCHEMA)
name_basics_df = read_tsv(spark, name_basics_path, NAME_BASICS_SCHEMA)
title_principals_df = read_tsv(spark, title_principals_path, TITLE_PRINCIPALS_SCHEMA)

print("TITLE_BASICS")
title_basics_df.printSchema()
title_basics_df.show(5, truncate=False)

print("TITLE_RATINGS")
title_ratings_df.printSchema()
title_ratings_df.show(5, truncate=False)

print("NAME_BASICS")
name_basics_df.printSchema()
name_basics_df.show(5, truncate=False)

print("TITLE_PRINCIPALS")
title_principals_df.printSchema()
title_principals_df.show(5, truncate=False)

spark.stop()
