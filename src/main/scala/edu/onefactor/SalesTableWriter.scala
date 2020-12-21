package edu.onefactor

import org.apache.spark.sql.SparkSession

import com.datastax.spark.connector._

object SalesTableWriter {
	def main(args: Array[String]) = {
		val spark = SparkSession.builder()
				// can be also specified using spark-submit --conf spark.cassandra.connection.host=localhost
				.config("spark.cassandra.connection.host", "localhost")
				.appName("SalesTableWriter")
				.getOrCreate()

		// reading from files
		val sales = spark.read.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load("/Volumes/Data/Users/andrey/Documents/Scala/3rdparty/spark-def-guide/data/retail-data/by-day/*.csv")
				.where("Description is not null and CustomerID is not null")
				
		sales.rdd
				.map(r => (r.getString(0), r.getString(1), r.getString(2), r.getInt(3), r.getString(4), r.getDouble(5), r.getDouble(6), r.getString(7)))
				// actually AllColumns can be used here
				.saveToCassandra("retail", "sales", SomeColumns("invoice_no", "stock_code", "description", "quantity", "invoice_date", "unit_price", "customer_id", "country"))

		println("completed")
	}
}