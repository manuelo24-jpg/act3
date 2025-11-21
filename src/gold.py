from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min, max, when, avg, count, datediff, unix_timestamp,
    round, concat, lit, floor, format_number
)

spark = SparkSession.builder.appName("gold").getOrCreate()

books = spark.read.parquet("silver/books")

# Mostrar algunas filas (show() ya imprime por sí mismo)
books.show(5)

# Estadísticas principales: media, máximo y mínimo de precio
booksStatistics = books.agg(
    format_number(avg(col("precio")), 2).alias("precio_medio"),
    max(col("precio")).alias("precio_maximo"),
    min(col("precio")).alias("precio_minimo")
)
booksStatistics.show()

# Seleccionar los 5 libros más caros con su título al lado
top5Books = (
    books
    .select(col("titulo"), col("precio"))
    .orderBy(col("precio").desc())
    .limit(5)
)
print("Top 5 libros más caros:")
top5Books.show(truncate=False)

booksStatistics.write.mode("overwrite").parquet("gold/books_statistics")
top5Books.write.mode("overwrite").parquet("gold/top5_libros")

spark.stop()
