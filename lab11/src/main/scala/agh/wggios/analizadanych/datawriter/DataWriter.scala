package agh.wggios.analizadanych.datawriter
import org.apache.spark.sql.DataFrame

class DataWriter {
  def write_df(df: DataFrame, path: String): Int={
    if(df.isEmpty){
      println("Cannot save empty Data Frame")
      -1
    }else{
      df.write.parquet(path)
      1
    }
  }
}
