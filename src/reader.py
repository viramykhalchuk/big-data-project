from pyspark.sql import SparkSession

def read_tsv(spark, path, schema):
    return (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .option("nullValue", "\\N")
        .schema(schema)
        .csv(str(path))
    )
