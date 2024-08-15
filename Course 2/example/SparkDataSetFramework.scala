package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

private case class Person(name:String, age:Long)

object SparkDataSetFramework {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();
    val spark1 = SparkSession.builder().master("local").appName("Test1").getOrCreate();

    import spark.implicits._

    //read people
    val peopleDf = spark.read.json("/Users/serkan/Desktop/Training3/data/peopleage.json")
    val peopleDf1 = spark1.read.json("/Users/serkan/Desktop/Training3/data/peopleage.json")

    val unDf = peopleDf1.union(peopleDf);
    unDf.show()

    peopleDf.printSchema()
    peopleDf.show()

    println("DataFrame use getLong/String")
    peopleDf.foreach(row => {
      println(s"Age:${row.getLong(0)}, City:${row.getString(1)}")
    })

    println("DataFrame use getAs")
    peopleDf.foreach(row => {
      println(s"Age:${row.getAs("age")}, City:${row.getAs("city")}")
    })

   val peopleDs =  peopleDf.as[Person]

    println("DataSet using field")
    peopleDs.foreach(row => {
      println(s"Age: ${row.age}, name:${row.name}")
    })

    println("Session 2 ---------->")
    peopleDf1.show()





  }

}
