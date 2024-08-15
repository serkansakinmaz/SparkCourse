package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkParquetRead {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    val userDf = spark.read.parquet("/Users/serkan/Desktop/Training3/data/userdata1.parquet")

    userDf.show()

    userDf.printSchema()

    userDf.createOrReplaceTempView("user")

    println("Use view ----------- > ")

    val canadaDf = spark.sql("select id,first_name,email from user where country='Canada'")

    //canadaDf.write.parquet("/Users/serkan/Desktop/Training3/data/parquet/2014/08/14")


  }

}
