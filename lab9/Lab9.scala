// Databricks notebook source
val fileName = "dbfs:/FileStore/tables/Nested.json"
val df = spark.read.option("multiline","true").json(fileName)

display(df)

// COMMAND ----------

val df2 = df.withColumn("pathLinkInfoEdited", $"pathLinkInfo". dropFields("startNode", "alternateName", "endNode"))

display(df2)

// COMMAND ----------

import org.apache.spark.sql.functions._

val sourceDF = Seq(
  ("  p a   b l o", "Paraguay"),
  ("Neymar", "B r    asil")
).toDF("name", "country")

val actualDF = Seq(
  "name",
  "country"
).foldLeft(sourceDF) { (memoDF, colName) =>
  memoDF.withColumn(
    colName,
    regexp_replace(col(colName), "\\s+", "")
  )
}

display(actualDF)


// COMMAND ----------

display(sourceDF)
