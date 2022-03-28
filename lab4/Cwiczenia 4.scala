// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val tables = tabela.where("TABLE_SCHEMA == 'SalesLT'")
val names = tables.select("TABLE_NAME").as[String].collect.toList
for (i <- names){
  //print(i +'\n')
  val SalesLt = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.$i")
    .load()
  
  SalesLt.write.format("delta").mode("overwrite").saveAsTable(i)
}
display(SalesLt)

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

//W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{ count(when(col(c).isNull, c)).alias(c)})
}
for (i <- names){
  val df = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.$i")
    .load()
  
  print("Number of nulls in each column in " + i + " table\n")
  df.select(countCols(df.columns):_*).show()
}

// COMMAND ----------

//Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
for( i <- names){
  val df = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.$i")
    .load()
  
  val df2 = df.na.fill("___").show()
}

// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle
for( i <- names){
  val df = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.$i")
    .load()
  
  val df2 = df.na.drop("any").show()
}

// COMMAND ----------

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
  val df = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.SalesOrderHeader")
    .load()
  

val suma = df.agg(sum("TaxAmt"), sum("Freight")).show()

val minimum = df.agg(min("TaxAmt"), min("Freight")).show()

val maximum = df.agg(max("TaxAmt"), max("Freight")).show()


// COMMAND ----------

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
//Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

  val df = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.Product")
    .load()


val df2 = df.groupBy("ProductModelID","Color","ProductCategoryID").agg(Map("Size" -> "max","Weight"->"avg"))
display(df2)


// COMMAND ----------

//Stwórz 3 funkcje UDF do wybranego zestawu danych,
//a.	Dwie funkcje działające na liczbach, int, double
//b.	Jedna funkcja na string
import org.apache.spark.sql.types._
  val df = spark.read
    .format("jdbc")
    .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
    .option("user","sqladmin")
    .option("password","$3bFHs56&o123$")
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("query",s"SELECT * FROM SalesLT.Product")
    .load()

//display(df)

val square = udf((x:Int) => x*x)
val round = udf((x:Double) => x.toInt)
val toLower = udf((x:String) => x.toLowerCase)

val df2 = df.withColumn("squared", square($"ProductCategoryID"))
            .withColumn("rounded", round($"ListPrice"))
            .withColumn("toLower", toLower($"Name"))
display(df2)

// COMMAND ----------

//Flatten json, wybieranie atrybutów z pliku json.
//  /FileStore/tables/brzydki.json
val json = spark.read.option("multiline", "true").json("/FileStore/tables/brzydki.json")
display(json)
