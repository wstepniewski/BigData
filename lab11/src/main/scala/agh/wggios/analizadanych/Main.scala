package agh.wggios.analizadanych
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.caseclass.FlightCaseClass
import agh.wggios.analizadanych.transformations.Transformations
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger


object Main {
  val logger: Logger = Logger.getRootLogger
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    logger.info("----------------------------------------------------------------------------------")
    logger.info("Test log.")
    //val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
    //import spark.implicits._
    val df = new DataReader().read_csv("2010-summary.csv").as[FlightCaseClass]
    df.filter(row => new Transformations().USA_flights(row)).show()
    logger.info("Everything done correctly.")

  }
}
