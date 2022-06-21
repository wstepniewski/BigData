// Databricks notebook source
// MAGIC %md
// MAGIC 1. Pobierz dane Spark-The-Definitive_Guide dostępne na github
// MAGIC 2. Użyj danych do zadania '../retail-data/all/online-retail-dataset.csv'

// COMMAND ----------

val file = "dbfs:/FileStore/tables/online_retail_dataset.csv"
var df = spark.read.format("csv")
            .option("header","true")
            .load(file)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Zapisz DataFrame do formatu delta i stwórz dużą ilość parycji (kilkaset)
// MAGIC * Partycjonuj po Country

// COMMAND ----------

val path = "dbfs:/FileStore/ratail_data_folder"

df.repartition(200)
  .write.format("delta")
  .partitionBy("Country")
  .mode("overwrite")
  .save(path)


// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS online_retail_dataset")

spark.sql(s"""
  CREATE TABLE online_retail_dataset
  USING Delta
  LOCATION 'dbfs:/FileStore/ratail_data_folder'
""")

// COMMAND ----------

spark.sql("select * from online_retail_dataset").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1: OPTIMIZE and ZORDER
// MAGIC 
// MAGIC Wykonaj optymalizację do danych stworzonych w części I `../delta/retail-data/`.
// MAGIC 
// MAGIC Dane są partycjonowane po kolumnie `Country`.
// MAGIC 
// MAGIC Przykładowe zapytanie dotyczy `StockCode`  = `22301`. 
// MAGIC 
// MAGIC Wykonaj zapytanie i sprawdź czas wykonania. Działa szybko czy wolno 
// MAGIC 
// MAGIC Zmierz czas zapytania kod poniżej - przekaż df do `sqlZorderQuery`.

// COMMAND ----------

// TODO
def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

val sqlZorderQuery = timeIt(spark.sql("select * from  online_retail_dataset where StockCode = '22301'").collect())

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from online_retail_dataset where StockCode = '22301'

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Skompaktuj pliki i przesortuj po `StockCode`.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- wypelnij
// MAGIC OPTIMIZE wypełnij
// MAGIC ZORDER by (wypełnij)

// COMMAND ----------

// MAGIC %md
// MAGIC Uruchom zapytanie ponownie tym razem użyj `postZorderQuery`.

// COMMAND ----------

// TODO
val poZorderQuery = timeIt(spark.sql("<tutaj zapytanie>").collect())

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2: VACUUM
// MAGIC 
// MAGIC Policz liczbę plików przed wykonaniem `VACUUM` for `Country=Sweden` lub innego kraju

// COMMAND ----------

// TODO
val plikiPrzed = dbutils.fs.ls(<adres ścieżki>).length

// COMMAND ----------

// MAGIC %md
// MAGIC Teraz wykonaj `VACUUM` i sprawdź ile było plików przed i po.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC VACUUM wypełnij

// COMMAND ----------

// MAGIC %md
// MAGIC Policz pliki dla wybranego kraju `Country=Sweden`.

// COMMAND ----------

// TODO
val plikiPo = dbutils.fs.ls(<ścieżka>).length

// COMMAND ----------

// MAGIC %md
// MAGIC ## Przeglądanie histrycznych wartośći
// MAGIC 
// MAGIC możesz użyć funkcji `describe history` żeby zobaczyć jak wyglądały zmiany w tabeli. Jeśli masz nową tabelę to nie będzie w niej history, dodaj więc trochę danych żeby zoaczyć czy rzeczywiście się zmieniają. 

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history <nazw tabeli>
