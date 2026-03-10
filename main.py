from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab1_test").getOrCreate()

data = [
    (1, "Anna", 20),
    (2, "Oleh", 21),
    (3, "Ira", 19)
]

df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()

spark.stop()