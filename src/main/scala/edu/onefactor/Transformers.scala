package edu.onefactor

import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.SQLTransformer

object Transformers extends Serializable {
	def main(args: Array[String]) = {
		// nospark session config can be also defined here
		// i.e. config("spark.sql.warehouse.dir", "/data")
		val spark = SparkSession.builder()
				.appName("Transformers")
				.getOrCreate()

		// UDF can be registered here

		// reading from files
		val sales = spark.read.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load("/Volumes/Data/Users/andrey/Documents/Scala/3rdparty/spark-def-guide/data/retail-data/by-day/*.csv")
				.where("Description is not null and CustomerID is not null")

		// creating sql transformer
		val totalPriceTransformer = new SQLTransformer()
				.setStatement("select Quantity * UnitPrice from __THIS__")
				
		// rdd.getNumPartitions is much better choice here
		println(s"sales data partitions number ${sales.rdd.partitions.size}")
				
		// transforming
		val totalPrices = totalPriceTransformer
				.transform(sales)
				.rdd
				.saveAsTextFile("/Volumes/Data/Users/andrey/Documents/Scala/spark/1factor/sales-total")
		
		println("completed")
	}
}
