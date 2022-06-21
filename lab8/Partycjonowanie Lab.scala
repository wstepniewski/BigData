// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
))

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")
sc.defaultParallelism

// COMMAND ----------

val file = "/FileStore/tables/pageviews_by_second.tsv"

var df = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(file)

val path = "dbfs:/wikipedia.parquet"

df.write.mode("overwrite").parquet(path)

df.explain
df.count()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(1)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(7)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(9)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(16)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(24)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(96)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(200)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(4000)  
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(6)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(5)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(4)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(3)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(2)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(1)
  .groupBy("site")
  .sum()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet"

val df = spark.read
  .parquet(parquetDir)
.repartition(2000)
//    .coalesce(6)
.groupBy("site").sum()


df.explain
df.count()
printRecordsPerPartition(df)

// COMMAND ----------

df.rdd.getNumPartitions
