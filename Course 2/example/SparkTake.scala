package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkTake {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //read text
    val rdd = spark.sparkContext.textFile("/Users/serkan/Desktop/Training3/data/warn")

    //filter rdd
    val filteredRdd =  rdd.filter(line => line.contains("WARN"))

    val output = filteredRdd.take(2)
    println(s"Number of records = ${output.length}")

  }

}
