package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

import java.util.Properties

case class Author(id:Int, name:String, email:String)

object SparkMySQLWrite {

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
    import spark.implicits._

    val authorsDf = spark.read.format("jdbc").option("url", connectionString)
      .option("dbtable", table).option("user",user).option("password", password)
      .option("driver","com.mysql.cj.jdbc.Driver").load()

    val author3 = new Author(3,"Ayse", "ayse@gmail.com")
    val author4 = new Author(4,"Mehmet", "mehmet@gmail.com")

    val authors = Seq(author3, author4)

    val authorEncoder = Encoders.bean(Author.getClass)

    val personDs = spark.createDataset(authors).as(authorEncoder)


    val connectionProperties = new Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password","")

    personDs.write.mode(SaveMode.Append).jdbc(connectionString,table, connectionProperties)

  }

}
