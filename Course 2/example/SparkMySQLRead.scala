package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkMySQLRead {

  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val database = "books5"
    val table = "authors"
    val user = "root"
    val password = ""
    val connectionString = "jdbc:mysql://localhost:3306/" + database

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    val authorsDf = spark.read.format("jdbc").option("url", connectionString)
      .option("dbtable", table).option("user",user).option("password", password)
      .option("driver","com.mysql.cj.jdbc.Driver").load()

    authorsDf.show()

    authorsDf.printSchema()

    authorsDf.createOrReplaceTempView("authors")

    spark.sql("select * from authors where id==1").show()

  }

}
