from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

def get_numeric_columns(df):
    return [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)]

def print_overview(df, name):
    print(f"\n=== {name}: OVERVIEW ===")
    print(f"rows: {df.count()}")
    print(f"columns: {len(df.columns)}")
    print(f"column_names: {df.columns}")
    df.printSchema()

def print_numeric_stats(df, name):
    numeric_cols = get_numeric_columns(df)
    print(f"\n=== {name}: NUMERIC STATS ===")
    if numeric_cols:
        df.select(numeric_cols).summary(
            "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
        ).show(truncate=False)
    else:
        print("No numeric columns")

def print_missing_values(df, name):
    print(f"\n=== {name}: MISSING VALUES ===")
    exprs = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    df.select(exprs).show(truncate=False)

def print_duplicate_count(df, name, subset=None):
    print(f"\n=== {name}: DUPLICATES ===")
    total_rows = df.count()
    distinct_rows = df.dropDuplicates(subset).count()
    print(f"total_rows: {total_rows}")
    print(f"distinct_rows: {distinct_rows}")
    print(f"duplicate_rows: {total_rows - distinct_rows}")

def print_column_informativeness(df, name):
    print(f"\n=== {name}: COLUMN INFORMATIVENESS ===")
    total_rows = df.count()
    stats = []
    for c in df.columns:
        non_null = df.filter(F.col(c).isNotNull()).count()
        distinct_count = df.select(c).distinct().count()
        fill_ratio = round(non_null / total_rows, 6) if total_rows else 0
        stats.append((c, non_null, distinct_count, fill_ratio))
    result = df.sparkSession.createDataFrame(
        stats,
        ["column", "non_null_count", "distinct_count", "fill_ratio"]
    )
    result.orderBy("fill_ratio", "distinct_count").show(truncate=False)
