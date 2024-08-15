package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkRdd {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    val rdd = spark.sparkContext.textFile("/Users/serkan/Desktop/Training3/data/README.txt")
    val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday",
      "Friday", "Saturday")

    val rddDays = spark.sparkContext.parallelize(days);

    println(s"Count of days: ${rddDays.count()}")

    val count = rdd.count()
    val first = rdd.first()

    println(s"Count:${count} , First record:${first}")

  }

}
