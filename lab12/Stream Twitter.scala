// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC Dane z servera Kafka pochodzą z Twittera
// MAGIC 
// MAGIC 
// MAGIC 0. Zmiejsz partycje shuffle do 4 
// MAGIC 0. Typ streamu Kafka
// MAGIC 0. Lokalizacja serverów  **server1.databricks.training:9092** (US-Oregon) - **server2.databricks.training:9092** (Singapore)
// MAGIC 0. Topic "subscribe" to "tweets"
// MAGIC 0. Throttle Kafka's processing of the streams (maxOffsetsPerTrigger)
// MAGIC 0. Opcja przy ponownym uruchomieniu notatnika przewiń strumień do początku (startingOffsets)
// MAGIC 0. Załaduj dane 
// MAGIC 0. Wybież kolumne `value` cast do typu `STRING`

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 4)

val kafkaServer = "server1.databricks.training:9092,server2.databricks.training:9092"

val twittsDF = spark.readStream                        
 .format("kafka")                                     // 2. 
 .option("kafka.bootstrap.servers", kafkaServer)      // 3.
 .option("subscribe", "tweets")                       // 4.
 .option("maxOffsetsPerTrigger", 1000)                // 5. 
 .option("startingOffsets", "earliest")               // 6. 
 .load()                                              // 7. 
 .select($"value".cast("STRING"))                     // 8. 

// COMMAND ----------

// MAGIC %md
// MAGIC * Sprawdź czy działa

// COMMAND ----------

twittsDF.isStreaming

// COMMAND ----------

// MAGIC %md
// MAGIC Schemat danych

// COMMAND ----------


import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, ArrayType}

lazy val twitSchema = StructType(List(
  StructField("hashTags", ArrayType(StringType, false), true),
  StructField("text", StringType, true),   
  StructField("userScreenName", StringType, true),
  StructField("id", LongType, true),
  StructField("createdAt", LongType, true),
  StructField("retweetCount", IntegerType, true),
  StructField("lang", StringType, true),
  StructField("favoriteCount", IntegerType, true),
  StructField("user", StringType, true),
  StructField("place", StructType(List(
    StructField("coordinates", StringType, true), 
    StructField("name", StringType, true),
    StructField("placeType", StringType, true),
    StructField("fullName", StringType, true),
    StructField("countryCode", StringType, true)
  )), true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC  JSON DataFrame
// MAGIC 
// MAGIC * Użyj `twittsDF` i sparsuj dane uzywając `from_json`. 
// MAGIC * Stwórz DataFrame, z poniższymi polami
// MAGIC * `time` (już podany)
// MAGIC * Dodaj kolumnę `json`, która pochodzi z kolumny `value`
// MAGIC * Wypłaszcz (flatten) pola jakie wystąpią w kolumnie `json`

// COMMAND ----------

import org.apache.spark.sql.functions.{from_json, expr, col}

val analizaDF = twittsDF
 .withColumn("json", from_json($"value", twitSchema))                         // tutaj parse kolumne "value"
 .select(
   expr("cast(cast(json.createdAt as double)/1000 as timestamp) as time"),  
   $"json.hashTags".as("hashTags"),                                           // Wyciągnij pola z kolumny "json"
   col("json.text").as("text"),
   col("json.userScreenName").as("userScreenName"),
   col("json.id").as("id"),
   col("json.createdAt").as("createdAt"),
   col("json.retweetCount").as("retweetCount"),
   col("json.lang").as("lang"),
   col("json.favoriteCount").as("favoriteCount"),
   col("json.user").as("user"),
   col("json.place.coordinates").as("coordinates"), 
   col("json.place.name").as("name"),
   col("json.place.placeType").as("placeType"),
   col("json.place.fullName").as("fullName"),
   col("json.place.countryCode").as("countryCode")
 )

// COMMAND ----------

// MAGIC %md
// MAGIC * Wyświetl dane 

// COMMAND ----------

display(analizaDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Zatrzymaj stream

// COMMAND ----------

for(stream <- spark.streams.active) 
  stream.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Obróbka hashtagów
// MAGIC 
// MAGIC * Dodaj kolumę 'hashTag', która podzieli kolumnę Hashtags na wiele wierszy  
// MAGIC * Zmień wszystkie hashtagi do 'lower case' 
// MAGIC * Grupuj po hashtagu i policz ile ich jest
// MAGIC * Posortuj dane po ilości malejąco 
// MAGIC * wyciągnij 30 najpopularniejszych hashtagów

// COMMAND ----------

import org.apache.spark.sql.functions.{lower, explode, desc}

val najpopularniejszeHashtagiDF = analizaDF 
 .select(explode(col("hashTags")).as("hashTag"), lower(col("hashTag")))
 .groupBy(col("hashTag"))
 .count()   
 .sort(desc("count"))
 .limit(30)
 

// COMMAND ----------

// MAGIC %md
// MAGIC Pokaż na wykresie wynik z najpopularniejszeHashtagiDF

// COMMAND ----------

display(najpopularniejszeHashtagiDF)

// COMMAND ----------

// MAGIC %md
// MAGIC * Wstrzymaj stream

// COMMAND ----------

for(stream <- spark.streams.active)
  stream.stop

// COMMAND ----------

// MAGIC %md
// MAGIC Zapisz stream
// MAGIC * Użyj formatu tabeli sink jako `in-memory`
// MAGIC * Output mode "append"
// MAGIC * Nazwij query
// MAGIC * Skonfiguruj wyzwalacz - co 10 sekund 
// MAGIC * Uruchom query

// COMMAND ----------

nazwaQuery = najpopularniejszeHashtagiDF 
.<wypełnij>                               
.<wypełnij>                               
.<wypełnij>                               
.<wypełnij>                               
.<wypełnij>                               
.<wypełnij>                               

// COMMAND ----------

// MAGIC %md
// MAGIC Wyłącz stream

// COMMAND ----------

for(stream <- spark.streams.active)
  stream.stop<wypełnij>
