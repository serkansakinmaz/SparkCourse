package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkFlatMap {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //read movies
    val moviesRddString = spark.sparkContext.textFile("/Users/serkan/Desktop/Training3/data/movies.csv")
    println(s"First : ${moviesRddString.first()}")
    println(s"Count : ${moviesRddString.count()}")

    val flatMapRdd = moviesRddString.flatMap(line => {
      (line.split(","))
    }
    )
    println(s"First : ${flatMapRdd.first()}")
    println(s"Count : ${flatMapRdd.count()}")




  }

}