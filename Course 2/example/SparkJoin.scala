package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkJoin {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //read movies
    val moviesRddString = spark.sparkContext.textFile("/Users/serkan/Desktop/Training3/data/movies.csv")

    val moviePairRdd = moviesRddString.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1))
    }
    )

    println(s"First : ${moviePairRdd.first()}")

    val samplePairRdd = moviePairRdd.sample(false, 0.1)

    val joinedRdd = moviePairRdd.join(samplePairRdd);

    println(s"Count: ${joinedRdd.count()}")
    println(s"First: ${joinedRdd.first()}")


  }

}
