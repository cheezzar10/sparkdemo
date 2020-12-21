package edu.onefactor

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path

object HdfsOpsApp {
    def main(args: Array[String]): Unit = {
        println("started")

        val spark = SparkSession.builder().getOrCreate()

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        for (e <- fs.listStatus(new Path("/shared/andrey.smirnov"))) {
            println(s"dir entry: $e")
        }

        // val dirCreated = fs.mkdirs(new Path("/shared/andrey.smirnov/batch_test"))

        // fails with exception due to missing permissions
        // val dirCreated = fs.mkdirs(new Path("/tmp/hadoop-yarn/testdir"))

        val dirCreated = fs.mkdirs(new Path("/shared/andrey.smirnov/test-entry"))

        println(s"dir created: $dirCreated")
    }
}