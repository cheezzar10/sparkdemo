package edu.onefactor

import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.functions._

class AggregationOpsSuite extends FunSuite with BeforeAndAfterAll {
    // TODO add spark session lifecycle management beforeTest/afterTest
    lazy val spark = SparkSession.builder()
        .appName("test")
        .master("local[1]")
        .config("spark.driver.allowMultipleContexts", "false")
        .getOrCreate()

    test("aggregation ops check on empty dataset") {
        val df = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("file:///Users/andrey.smirnov/Documents/projects/samples/spark-def-guide/data/retail-data/all/online-retail-dataset.csv")

        // df.show(truncate=false)

        import spark.implicits._

        val invoiceStats = df.where(col("InvoiceNo") === "a")
            // .withColumn("partition", spark_partition_id())
            .groupBy("partition")
            .agg(count("*").as("rows"), countDistinct("InvoiceDate").as("dates"))
            .groupBy() // can be dropped and next line can be replaced with select(max("rows").as("max_rows"), max("dates").as("max_dates"))
            .agg(max("rows"), max("dates"))
            .as[(Option[Long], Option[Long])]
            .collect
        
        println(s"Collected stats: ${invoiceStats.mkString}")
    }

    override def afterAll(): Unit = {
        spark.stop()
    }
}