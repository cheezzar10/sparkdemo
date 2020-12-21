package edu.onefactor

import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path

class SparkSessionOpsSuite extends FunSuite {
    val spark = SparkSession.builder()
        .appName("test")
        .master("local[1]")
        .config("spark.driver.allowMultipleContexts", "false")
        .getOrCreate()

    test("spark session should be created successfully") {
        val fs = FileSystem.get(
            URI.create("/shared/andrey.smirnov"), 
            spark.sparkContext.hadoopConfiguration)

        val entries = fs.listStatus(new Path("/"))

        for (e <- entries) {
            println(s"dir entry: $e")
        }
    }
}