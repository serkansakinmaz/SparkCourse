package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object SparkAls {

  private case class Rating(userId: Int, movieId: Int, rating: Double)

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();
    import spark.implicits._

    val rddStr = spark.sparkContext.textFile("/Users/serkan/Desktop/Training3/data/ratings.csv")


    println(s"First:${rddStr.first()}")
    println(s"Count:${rddStr.count()}")

    //convert string rdd to movie rdd
    val ratingRdd = rddStr.map { line =>
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }

    val splits = ratingRdd.randomSplit(Array(0.8,0.2))
    val trainingDf = splits(0).toDF()


    //define model
    val als = new ALS().setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    //create ALS model
    val model = als.fit(trainingDf)

    val recommendation = model.recommendForAllUsers(5)



    recommendation.foreach(row => {
      println(row)
    })

  }

}
