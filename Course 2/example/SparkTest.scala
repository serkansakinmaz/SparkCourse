package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkTest {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    //dosya okuma
    val file = spark.read.textFile("/Users/serkan/Desktop/Training3/data/README.txt")

    //icerigi goster
    file.show()
  }

}
