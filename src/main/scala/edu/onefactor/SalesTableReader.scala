package edu.onefactor

import org.apache.spark.sql.SparkSession

import com.datastax.spark.connector._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.Row

object SalesTableReader {
	def main(args: Array[String]) = {
		val spark = SparkSession.builder()
				// application name should be specified in spark-submit --name SalesTableReader
				.appName("SalesTableReader")
				.getOrCreate()
				
		/*
		val salesRdd = spark.sparkContext
				// .cassandraTable[Row]("retail", "sales")
				.cassandraTable[(Int, Double)]("retail", "sales")
				.select("quantity", "unit_price")
				.map { case (q, up) => Row(q, up) }
				// .map(r => Row(r._1, r._2))
		*/
				
		// actually spark-cassandra-connector Datasource V1 ( for spark < 3.0 ) can be used to obtain DataFrame directly
		val salesRdd = spark.sparkContext
				.cassandraTable("retail", "sales")
				.select("quantity", "unit_price")
				.map(r => Row(r.getInt("quantity"), r.getDouble("unit_price")))
		
		println(s"cassandra table partitions: ${salesRdd.getNumPartitions}")

		val sales = spark.createDataFrame(salesRdd, StructType(Array(
				new StructField(name = "quantity", dataType = IntegerType, nullable = false), 
				new StructField(name = "unit_price", dataType = DoubleType, nullable = false))
		))

		val totalPriceTransformer = new SQLTransformer()
				.setStatement("select quantity * unit_price from __THIS__")
				
		// transforming
		val totalPrices = totalPriceTransformer
				.transform(sales)
				.rdd
				.saveAsTextFile("/Volumes/Data/Users/andrey/Documents/Scala/spark/1factor/sales-total")

		println("completed")
	}
}