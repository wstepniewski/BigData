// Databricks notebook source
// MAGIC %sql
// MAGIC Create database if not exists Sample

// COMMAND ----------

spark.sql("Create database if not exists Sample")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField, DateType, DoubleType, ShortType}
val Transactions_schema = StructType(Array(
  StructField("AccountId", IntegerType, true),
  StructField("TranDate", DateType, true),
  StructField("TranAmt", DoubleType, true)
))

val Logical_schema = StructType(Array(
  StructField("RowId", IntegerType, true),
  StructField("FName", StringType, true),
  StructField("Salary", IntegerType, true)
))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC INSERT INTO Sample.Transactions VALUES 
// MAGIC ( 1, '2011-01-01', 500),
// MAGIC ( 1, '2011-01-15', 50),
// MAGIC ( 1, '2011-01-22', 250),
// MAGIC ( 1, '2011-01-24', 75),
// MAGIC ( 1, '2011-01-26', 125),
// MAGIC ( 1, '2011-01-28', 175),
// MAGIC ( 2, '2011-01-01', 500),
// MAGIC ( 2, '2011-01-15', 50),
// MAGIC ( 2, '2011-01-22', 25),
// MAGIC ( 2, '2011-01-23', 125),
// MAGIC ( 2, '2011-01-26', 200),
// MAGIC ( 2, '2011-01-29', 250),
// MAGIC ( 3, '2011-01-01', 500),
// MAGIC ( 3, '2011-01-15', 50 ),
// MAGIC ( 3, '2011-01-22', 5000),
// MAGIC ( 3, '2011-01-25', 550),
// MAGIC ( 3, '2011-01-27', 95 ),
// MAGIC ( 3, '2011-01-30', 2500)

// COMMAND ----------

import java.sql.Date
val Transactions_data: RDD[Row] = sc.parallelize(Seq(
        Row( 1, Date.valueOf("2011-01-01"), 500.0),
        Row( 1, Date.valueOf("2011-01-15"), 50.0),
        Row( 1, Date.valueOf("2011-01-22"), 250.0),
        Row( 1, Date.valueOf("2011-01-24"), 75.0),
        Row( 1, Date.valueOf("2011-01-26"), 125.0),
        Row( 1, Date.valueOf("2011-01-28"), 175.0),
        Row( 2, Date.valueOf("2011-01-01"), 500.0),
        Row( 2, Date.valueOf("2011-01-15"), 50.0),
        Row( 2, Date.valueOf("2011-01-22"), 25.0),
        Row( 2, Date.valueOf("2011-01-23"), 125.0),
        Row( 2, Date.valueOf("2011-01-26"), 200.0),
        Row( 2, Date.valueOf("2011-01-29"), 250.0),
        Row( 3, Date.valueOf("2011-01-01"), 500.0),
        Row( 3, Date.valueOf("2011-01-15"), 50.0),
        Row( 3, Date.valueOf("2011-01-22"), 5000.0),
        Row( 3, Date.valueOf("2011-01-25"), 550.0),
        Row( 3, Date.valueOf("2011-01-27"), 950.0),
        Row( 3, Date.valueOf("2011-01-30"), 2500.0)))

val Transactions_df = spark.createDataFrame(Transactions_data, Transactions_schema)
display(Transactions_df)

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Sample.Logical
// MAGIC VALUES (1,'George', 800),
// MAGIC (2,'Sam', 950),
// MAGIC (3,'Diane', 1100),
// MAGIC (4,'Nicholas', 1250),
// MAGIC (5,'Samuel', 1250),
// MAGIC (6,'Patricia', 1300),
// MAGIC (7,'Brian', 1500),
// MAGIC (8,'Thomas', 1600),
// MAGIC (9,'Fran', 2450),
// MAGIC (10,'Debbie', 2850),
// MAGIC (11,'Mark', 2975),
// MAGIC (12,'James', 3000),
// MAGIC (13,'Cynthia', 3000),
// MAGIC (14,'Christopher', 5000);

// COMMAND ----------

val Logical_data: RDD[Row] = sc.parallelize(Seq(
        Row(1, "George", 800),
        Row(2, "Sam", 950),
        Row(3, "Diane", 1100),
        Row(4, "Nicholas", 1250),
        Row(5, "Samuel", 1250),
        Row(6, "Patricia", 1300),
        Row(7, "Brian", 1500),
        Row(8, "Thomas", 1600),
        Row(9, "Fran", 2450),
        Row(10, "Debbie", 2850),
        Row(11, "Mark", 2975),
        Row(12, "James", 3000),
        Row(13, "Cynthia", 3000),
        Row(14, "Christopher", 5000)))

val Logical_df = spark.createDataFrame(Logical_data, Logical_schema)
display(Logical_df)

// COMMAND ----------

// MAGIC %md 
// MAGIC Totals based on previous row

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// MAGIC FROM Sample.Transactions ORDER BY AccountId, TranDate;

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val window = Window.partitionBy("AccountId").orderBy("TranDate")
val DF1 = Transactions_df.withColumn("RunTotalAmt",sum("TranAmt").over(window)).orderBy("AccountId", "TranDate")
display(DF1)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running average of all transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
// MAGIC -- running total # of transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
// MAGIC -- smallest of the transactions so far
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
// MAGIC -- largest of the transactions so far
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId,TranDate;

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate")
val DF2 = Transactions_df
.withColumn("RunAvg", avg("TranAmt").over(window))
.withColumn("RunTranQty", count("*").over(window))
.withColumn("RunSmallAmt", min("TranAmt").over(window))
.withColumn("RunLargeAmt", max("TranAmt").over(window))
.withColumn("RunTotalAmt", sum("TranAmt").over(window))
.orderBy("AccountId", "TranDate")

display(DF2)

// COMMAND ----------

// MAGIC %md 
// MAGIC * Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- average of the current and previous 2 transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
// MAGIC -- total # of the current and previous 2 transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
// MAGIC -- smallest of the current and previous 2 transactions
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
// MAGIC -- largest of the current and previous 2 transactions
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
// MAGIC -- total of the current and previous 2 transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId, TranDate, RN

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val window2 = Window.partitionBy("AccountId").orderBy("TranDate")
val DF3 = Transactions_df
  .withColumn("SlideAvg",avg("TranAmt").over(window))
.withColumn("SlideQty",count("*").over(window))
.withColumn("SlideMin",min("TranAmt").over(window))
.withColumn("SlideMax",max("TranAmt").over(window))
.withColumn("SlideTotal",sum("TranAmt").over(window))
.withColumn("RN", row_number().over(window2))
.orderBy("AccountId", "TranDate", "RN")
display(DF3)

// COMMAND ----------

// MAGIC %md
// MAGIC * Logical Window

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT RowID,
// MAGIC FName,
// MAGIC Salary,
// MAGIC SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
// MAGIC SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange,
// MAGIC 
// MAGIC FROM Sample.Logical
// MAGIC ORDER BY RowID;

// COMMAND ----------

val window = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val window2 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
val DF4 = Logical_df
  .withColumn("SumByRows",sum("Salary").over(window))
  .withColumn("SumByRange",sum("Salary").over(window2))
  .orderBy("RowID")
display(DF4)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT TOP 10
// MAGIC AccountNumber,
// MAGIC OrderDate,
// MAGIC TotalDue,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountNumber ORDER BY OrderDate) AS RN
// MAGIC FROM Sales.SalesOrderHeader
// MAGIC ORDER BY AccountNumber; 

// COMMAND ----------

val window = Window.partitionBy("TranAmt").orderBy("TranDate")

val DF5 = Transactions_df
    .withColumn("RN", row_number().over(window))
    .orderBy("TranAmt")
    .limit(10)

display(DF5)

// COMMAND ----------

// MAGIC %md 
// MAGIC #ZAD 2

// COMMAND ----------

val window = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val windowRows = window.rowsBetween(Window.unboundedPreceding, -2)

val DF6 = Transactions_df
    .withColumn("RunLead",lead("TranAmt",2).over(window))
    .withColumn("RunLag",lag("TranAmt",2).over(window))
    .withColumn("RunFirstValue",first("TranAmt").over(windowRows))
    .withColumn("RunLastValue",last("TranAmt").over(windowRows))
    .withColumn("RunRowNumber",row_number().over(window))
    .withColumn("RunDenseRank",dense_rank().over(window))
    .orderBy("AccountId", "TranDate")

display(DF6)

// COMMAND ----------

// MAGIC %md
// MAGIC #ZAD 3

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/actors.csv"
val actorsDf = spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(filePath)

display(actorsDf)

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/names.csv"
val namesDf = spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

val join_expression = actorsDf.col("imdb_name_id") === namesDf.col("imdb_name_id")

// COMMAND ----------

namesDf.join(actorsDf, join_expression, "left_semi").explain()

// COMMAND ----------

namesDf.join(actorsDf, join_expression, "left_anti").explain()

// COMMAND ----------

// MAGIC %md
// MAGIC #ZAD 4

// COMMAND ----------

val DF7 = namesDf.join(actorsDf, join_expression).drop(actorsDf.col("imdb_name_id"))
display(DF7)

// COMMAND ----------

val DF8 = namesDf.join(actorsDf, Seq("imdb_name_id"))
display(DF8)

// COMMAND ----------

// MAGIC %md
// MAGIC #ZAD 5

// COMMAND ----------

val DF9 = actorsDf.join(broadcast(namesDf),"imdb_name_id")
display(DF9)

// COMMAND ----------

DF9.explain()
