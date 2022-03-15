# Databricks notebook source
# MAGIC %md
# MAGIC ## Zad1

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField, DateType}
# MAGIC 
# MAGIC val schema = StructType(Array(
# MAGIC   StructField("imdb_name_id", StringType, false),
# MAGIC   StructField("name", StringType, false),
# MAGIC   StructField("birth_name", StringType, false),
# MAGIC   StructField("height", IntegerType, true),
# MAGIC   StructField("bio", StringType, false),
# MAGIC   StructField("birth_details", StringType, false),
# MAGIC   StructField("date_of_birth", DateType, true),
# MAGIC   StructField("place_of_birth", StringType, true),
# MAGIC   StructField("death_details", StringType, true),
# MAGIC   StructField("date_of_death", DateType, true),
# MAGIC   StructField("place_of_death", StringType, true),
# MAGIC   StructField("reason_of_death", StringType, true),
# MAGIC   StructField("spouses_string", StringType, true),
# MAGIC   StructField("spouses", IntegerType, false),
# MAGIC   StructField("divorces", IntegerType, false),
# MAGIC   StructField("spouses_with_children", IntegerType, false),
# MAGIC   StructField("children", IntegerType, false)))
# MAGIC 
# MAGIC val filePath = "dbfs:/FileStore/tables/Files/names.csv"
# MAGIC 
# MAGIC val dataFrame = spark.read.format("csv").option("header", "true").schema(schema).load(filePath)
# MAGIC 
# MAGIC display(dataFrame)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zad4

# COMMAND ----------

# MAGIC %scala
# MAGIC val row1 = "{'imdb_name_id':'nm0000001','name':'Fred Astaire','birth_name':'Frederic Austerlitz Jr.','height':177,'bio':'Fred Astaire was born in Omaha, Nebraska, to Johanna (Geilus) and Fritz Austerlitz, a brewer. ','birth_details':'May 10, 1899 in Omaha, Nebraska, USA','date_of_birth':'1899-05-10','place_of_birth':'Omaha, Nebraska, USA','death_details':'June 22, 1987 in Los Angeles, California, USA  (pneumonia)','date_of_death':'null','place_of_death':'Los Angeles, California, USA ','reason_of_death':'pneumonia','spouses_string':'Robyn Smith (27 June 1980 - 22 June 1987) ','spouses':2,'divorces':0,'spouses_with_children':1,'children':2}"
# MAGIC 
# MAGIC val row2 =   "{'imdb_name_id':'nm0000002','name':'Lauren Bacall','birth_name':'Betty Joan Perske','height':174,'bio':'Lauren Bacall was born Betty Joan Perske on September 16, 1924, in New York City.','birth_details':'September 16, 1924 in The Bronx, New York City, New York, USA','date_of_birth':'null','place_of_birth':'The Bronx, New York City, New York, USA','death_details':'August 12, 2014 in New York City, New York, USA  (stroke)','date_of_death':'null','place_of_death':'New York City, New York, USA','reason_of_death':'stroke','spouses_string':'Jason Robards (4 July 1961 - 10 September1969)','spouses':2,'divorces':1,'spouses_with_children':2,'children':3}"
# MAGIC 
# MAGIC val row3 = "{dfsdfdfffsfsfs}"
# MAGIC 
# MAGIC val filePath2 = "/FileStore/tables/zad4.json"
# MAGIC 
# MAGIC Seq(row1, row2, row3).toDF().write.mode("overwrite").text(filePath2)
# MAGIC 
# MAGIC val DF1 = spark.read.format("json")
# MAGIC   .schema(schema)
# MAGIC   .option("badRecordsPath", "/FileStore/tables/badrecords")
# MAGIC   .load(filePath2)
# MAGIC 
# MAGIC val DF2 = spark.read.format("json")
# MAGIC   .schema(schema)
# MAGIC   .option("mode", "PERMISSIVE")
# MAGIC   .load(filePath2)
# MAGIC 
# MAGIC val DF3 = spark.read.format("json")
# MAGIC   .schema(schema)
# MAGIC   .option("mode", "DROPMALFORMED")
# MAGIC   .load(filePath2)
# MAGIC 
# MAGIC val DF4 = spark.read.format("json")
# MAGIC   .schema(schema)
# MAGIC   .option("mode", "FAILFAST")
# MAGIC   .load(filePath2)
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zad5

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val newDataFrame = dataFrame.limit(5)
# MAGIC 
# MAGIC val json_path = "/FileStore/tables/DF.json"
# MAGIC newDataFrame.write.format("json").mode("overwrite").save(json_path)
# MAGIC val parquet_path ="/FileStore/tables/DF.parquet"
# MAGIC newDataFrame.write.format("parquet").mode("overwrite").save(parquet_path)
# MAGIC 
# MAGIC val DF_json = spark.read.format("json").schema(schema).load(json_path)
# MAGIC //wszystko wyglada dobrze
# MAGIC 
# MAGIC val DF_parquet = spark.read.format("parquet").schema(schema).load(parquet_path)
# MAGIC //tu również wszystko wygląda dobrze
