package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkDataSet {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //read text
    val peopleDf = spark.read.json("/Users/serkan/Desktop/Training3/data/peopleage.json")

    peopleDf.show()
    peopleDf.printSchema()

    println("Use Select ---------")
    val filteredDf = peopleDf.select("name","age")
    filteredDf.show()

  }

}
