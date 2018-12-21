package ru.kai.lessons.lesson3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author alexander.leontyev
  */
object Task {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\dev\\winutils\\bin")

    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("market-basket-problem")
    conf.setMaster("local[2]")

    //SparkContext
    val sc = new SparkContext(conf)

    val inputFile = "retails.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Read file to DF
    val retails = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)

    retails.show(10)
    retails.printSchema()
  }
}
