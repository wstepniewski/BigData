package agh.wggios.analizadanych.datareader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Paths}

class DataReader {
  def read_csv(path: String): DataFrame = {
    val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
    if(Files.exists(Paths.get(path))) {
      spark.read.format("csv").option("header", value = true).option("inferSchema",value = true).load(path)
    } else{
      println("File not found")
      System.exit(0)
      spark.emptyDataFrame
    }
  }
}
