// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()`
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date", "string_date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021","18-03-2000"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021","12-08-1998"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021","28-11-2123"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------


dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------


val zmianaFormatu = dataFrame
  .withColumnRenamed("timestamp", "xxxxx")
  .select( $"*", unix_timestamp($"xxxxx", "yyyy-MM-dd HH:mm:ss") )

zmianaFormatu.printSchema()


// COMMAND ----------


val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")

tempE.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

val extendedDataFrame = dataFrame.withColumn("year", year($"current_date"))
                                 .withColumn("month", month($"current_date"))
                                 .withColumn("day_of_year", dayofyear($"current_date"))

display(extendedDataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC ## date_format()

// COMMAND ----------

val DF1 = dataFrame.withColumn("newDate", date_format($"current_date", "MM-yyyy-dd"))

display(DF1)

// COMMAND ----------

// MAGIC %md
// MAGIC ## from_unixtime()

// COMMAND ----------

val DF2 = dataFrame.withColumn("time_from_unix", from_unixtime($"unix"))

display(DF2)

// COMMAND ----------

// MAGIC %md
// MAGIC ## to_date()

// COMMAND ----------

val DF3 = dataFrame.withColumn("to_date", to_date($"string_date", "dd-MM-yyyy"))

display(DF3)

// COMMAND ----------

// MAGIC %md
// MAGIC ## to_timestamp()

// COMMAND ----------

val DF4 = DF3.withColumn("to_timestamp", to_timestamp($"to_date"))

display(DF4)

// COMMAND ----------

// MAGIC %md
// MAGIC ## to_utc_timestamp()

// COMMAND ----------

val DF5 = dataFrame.withColumn("to_utc_timestamp", to_utc_timestamp($"current_date", "JST"))

display(DF5)

// COMMAND ----------

// MAGIC %md
// MAGIC ## from_utc_timestamp()

// COMMAND ----------

val DF6 = DF5.withColumn("from_utc_timestamp", to_utc_timestamp($"to_utc_timestamp", "JST"))

display(DF6)
