// Databricks notebook source
// MAGIC %md
// MAGIC #ZAD 1

// COMMAND ----------

// MAGIC %md
// MAGIC Hive ma ograniczone możliwości indeksowania. Nie ma kluczy, rozumianych w znaczeniu relacyjnych baz danych, ale może zbudować indeks na kolumnach by przyspieszyć pewne operacje. Dane o indeksach są przechowywane w oddzielnej tabeli.  

// COMMAND ----------

// MAGIC %md
// MAGIC #ZAD 3

// COMMAND ----------

spark.sql("CREATE DATABASE DB1")

// COMMAND ----------

spark.catalog.listDatabases().show()

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/names.csv"
val namesDf = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

namesDf.write.mode("overwrite").saveAsTable("db1.names")

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/actors.csv"
val actorsDf = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

actorsDf.write.mode("overwrite").saveAsTable("db1.actors")

// COMMAND ----------

spark.catalog.listTables("db1").show()

// COMMAND ----------

spark.sql("select * from db1.names limit 10").show()

// COMMAND ----------

spark.sql("select * from db1.actors limit 10").show()

// COMMAND ----------

import org.apache.spark.sql.types._

def clear_db(db: String){
  val tables = spark.catalog.listTables(s"$db").select("name").as[String].collect.toList
  for (table <- tables){
    spark.sql(s"delete from $db.$table")
  }
}

clear_db("db1")

// COMMAND ----------

spark.sql("select * from db1.actors").show()

// COMMAND ----------

spark.sql("select * from db1.names").show()

// COMMAND ----------


