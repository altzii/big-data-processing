package ru.kai.lessons.lesson2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author alexander.leontyev
  */
object Task6 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\dev\\winutils\\bin")

    val conf = new SparkConf()
    conf.setAppName("TaskA")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val inputFile = "data/task6.txt"

    val fileRDD = sc.textFile(inputFile)
    val enemies = fileRDD.map(s => s.split(";"))

    val enemies1 = enemies.map(array => (array(1), 1))
    val enemiesCount = enemies1.reduceByKey((a, b) => a + b)

    val enemies2 = enemies.map(array => (array(1), array(2).toInt))
    val killedCOunt = enemies2.reduceByKey((a, b) => a + b)
  }
}
