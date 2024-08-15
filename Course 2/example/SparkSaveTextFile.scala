package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSaveTextFile {

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


    val comedyRdd = moviesRddString.filter(row => {
      //(row.contains("Comedy") && row.contains("Toy Story (1995)"))
      row.contains("Comedy")
    })

    println(comedyRdd.first())
    println(comedyRdd.count())

    comedyRdd.repartition(1).saveAsTextFile("/Users/serkan/Desktop/Training3/output/2024/08/14")






  }

}
