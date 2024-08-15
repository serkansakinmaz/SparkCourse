package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLGroupBy {

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

    println("Use Spark SQL with Filter")

    spark.sql("select * from people where city=='Lake Gladysberg'").show()

    println("Use Spark SQL with Filter   ------------------- > ")

    spark.sql("select city, count(1) from people group by city order by city").show()


    System.in.read()
    //Expect input
    spark.stop()
  }

}
