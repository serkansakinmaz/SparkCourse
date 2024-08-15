package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, percent_rank}

object SparkSQL {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //read text
    val peopleDf = spark.read.json("/Users/serkan/Desktop/Training3/data/people.json")

    peopleDf.show()

    peopleDf.printSchema()

    peopleDf.createOrReplaceTempView("people")

    println("Use Spark SQL")
    val peopleDf1 = spark.sql("select * from people limit 2")
    peopleDf1.show()


  }

}
