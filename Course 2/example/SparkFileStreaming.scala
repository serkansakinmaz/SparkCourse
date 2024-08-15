package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkFileStreaming {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate();

    val schema = StructType(
      Array(
        StructField("sn",StringType, true),
        StructField("type",IntegerType,true),
        StructField("value1",StringType,true),
        StructField("value2",StringType,true)
      )
    )

    val weatherDf = spark.readStream.option("sep",",").schema(schema).csv("/Users/serkan/Desktop/Training3/data/stream")

    val result = weatherDf.select("sn","value2").where("type == 1 AND value1=='+'" +
      " AND CAST(value2 AS INT) > 15")

    val query = result.writeStream.format("console").start()

    query.awaitTermination()



  }

}
