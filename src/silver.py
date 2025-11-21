from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, to_timestamp
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("silver").getOrCreate()

books   = spark.read.parquet("bronze/books")


books = (books
    .withColumn("precio",   col("precio").cast(DoubleType()))
)

print("Esquema de libros")
books.show(20)
books.write.mode("overwrite").parquet("silver/books")

print("Silver listo.")
spark.stop()


