package edu.onefactor

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.ml.feature.SQLTransformer

object SalesTableDatasetReader {
	def main(args: Array[String]) = {
		val spark = SparkSession.builder()
				.config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
				.withExtensions(new CassandraSparkExtensions)
				.getOrCreate()
				
		// actually you can use spark.sql()
		val df = spark.read.table("casscatalog.retail.sales")

		val totalPriceTransformer = new SQLTransformer()
				.setStatement("select quantity * unit_price from __THIS__")
				
		// transforming
		val totalPrices = totalPriceTransformer
				.transform(df.select(df.col("quantity"), df.col("unit_price")))
				.rdd
				.saveAsTextFile("/Volumes/Data/Users/andrey/Documents/Scala/spark/1factor/sales-total")
				
		println("completed")
	}
}