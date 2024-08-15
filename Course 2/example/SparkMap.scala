package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkMap {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //read movies
    val moviesRddString = spark.sparkContext.textFile("/Users/serkan/Desktop/Training3/data/movies.csv")

    val movieRdd = moviesRddString.map(line => {
      val arr = line.split(",")
      new Movie(arr(0), arr(1), arr(2),"Spark Course")
    }
    )

    println(s"First : ${movieRdd.first()}")

    //filter rdd

    val output = moviesRddString.take(2)
    println(s"Number of records = ${output.length}")

  }

}
