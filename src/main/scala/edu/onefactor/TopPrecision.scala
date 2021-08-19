package edu.onefactor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TopPrecision {
  private val TdsPath: String = "/shared/sm-data/MLS-3101/application_1627445160351_195961/tds_oot"
  // private val TdsPath: String = "/shared/andrey.smirnov/MLS-3101/tds"
  private val ScoredTdsPath: String = "/shared/sm-data/MLS-3101/application_1627445160351_195961/scored_tds"
  // private val ScoredTdsPath: String = "/shared/andrey.smirnov/MLS-3101/enriched_sample_tds"
  private val TopScoresLimit: Int = 5608

  def main(args: Array[String]): Unit = {
    println("started")

    val spark = SparkSession.builder().getOrCreate()
    // spark.sparkContext.setLogLevel("DEBUG")

    val tds = spark.read.parquet(TdsPath)

    val scoredTds = spark.read.parquet(ScoredTdsPath)
      .join(tds, Seq("msisdn_hash", "request_date"))
      .orderBy(col("score").desc)
      .drop(tds.col("target"))
      .cache()

    val topScores = scoredTds
      .select("target")
      .limit(TopScoresLimit)
      .select(avg("target"))

    topScores.show()
  }
}
