package edu.onefactor

import java.text.NumberFormat

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object ScoreDataframeReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val df = spark.read.parquet("/shared/andrey.smirnov/batch_test_float_null")

    val nf = NumberFormat.getInstance

    nf.setGroupingUsed(false)
    nf.setMaximumFractionDigits(6)
    nf.setMinimumFractionDigits(1)

    val fn = udf { n: Double => Option(n).map(nf.format).getOrElse("") }

    val fdf = df.withColumn("dummy_score", fn(df.col("dummy_score")))

    fdf.write
      .format("csv")
      .mode("overwrite")
      // null value quoting suppression
      .option("emptyValue", "")
      .option("header", "true")
      .save("file:///Users/andrey.smirnov/Downloads/formatted_score")

    println("results stored in csv file")
  }
}
