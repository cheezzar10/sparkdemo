package edu.onefactor

import org.apache.spark.sql.SparkSession
import java.nio.file.Files
import java.util.UUID
import scala.io.Source

object AggOpsApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()

        val runtimeConfProps = spark.conf.getAll

        println(s"application started - runtime configuration: $runtimeConfProps")

        val df = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("file:///Users/andrey.smirnov/Documents/projects/samples/spark-def-guide/data/retail-data/all/online-retail-dataset.csv")

        // df.show()
        println(s"df: ${df.toString}")

        val appName = runtimeConfProps("spark.app.name")

        println(s"df schema: ${df.schema}")
        for (row <- df.rdd.take(10)) {
            println(s"df row: $row")
        }

        sys.exit(0)

        // the following approach is not working due to the fact that saving rdd to file uses partitioning also
        val dfOutPath = Files.createTempFile(appName, UUID.randomUUID().toString)
        // spark refuses to perform output to existing file ( use create temp dir )
        Files.deleteIfExists(dfOutPath)
        try {
            println(s"temporary dataframe output file path: ${dfOutPath.toUri}")

            df.rdd.saveAsTextFile(dfOutPath.toUri.toString)

            // TODO close source
            for (line <- Source.fromFile(dfOutPath.toFile)) {
                println(s"line = [ $line ]")
            }
        } finally {
            val deleted = Files.deleteIfExists(dfOutPath)
            if (deleted) {
                println("temporary dataframe output file deleted")
            }
        }

        println("completed")
    }
}