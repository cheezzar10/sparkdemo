package edu.onefactor

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object DataframeWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val n = -1.0
    if (n.abs > 0.0) {
      throw new RuntimeException("planned job failure")
    }

    val dfSchema = StructType(Seq(
      StructField("request_date_str", StringType, false),
      StructField("msisdn_hash", StringType, false),
      StructField("dummy_score", FloatType, true)
    ))

    val dfRdd = spark.sparkContext.parallelize(Seq(
      Row("2020-10-28", "e+XFeR/604w=", 0.000479f),
      Row("2020-10-28", "9M2NJ7gYOsw=", 0.000641f),
      Row("2020-10-27", "fbMRCQ5K8Lc=", 0.0001f),
      Row("2020-10-27", "NXr9ETLgZK0=", null)
    ))

    import spark.implicits._

    val df = spark.createDataFrame(dfRdd, dfSchema)
      .withColumn("request_date", to_date($"request_date_str").alias("request_date"))
      .select("request_date", "msisdn_hash", "dummy_score")

    val time = System.currentTimeMillis

    df.write
      .mode(SaveMode.Overwrite)
      .parquet("/shared/andrey.smirnov/batch_test_" + time)

    println("mock dataframe saved")
  }
}
