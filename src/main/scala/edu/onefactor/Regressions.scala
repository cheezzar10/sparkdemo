package edu.onefactor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, DoubleType }
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline

object Regressions {
	def main(args: Array[String]) = {
		val spark = SparkSession.builder().getOrCreate()
		
		val schema = new StructType(Array(
				new StructField("dummy", DoubleType, false),
				new StructField("x", DoubleType, false),
				new StructField("y", DoubleType, false)
		))
		
		val df = spark.read
				.format("csv")
				.option("header", "false")
				.option("sep", "\t")
				.schema(schema)
				.load("/Volumes/Data/Users/andrey/Downloads/machinelearninginaction/Ch08/ex0.txt")
				
		val rf = new RFormula()
				.setFormula("y ~ x")
		
		// val frf = rf.fit(df)
		// val pdf = frf.transform(df)
				
		// here we can use setFeaturesCol & setLabelCol to avoid selection transformer
		val lr = new LinearRegression()
				.setLabelCol("label")
				.setFeaturesCol("features")

		val pl = new Pipeline().setStages(Array(rf, lr))
		
		val plm = pl.fit(df)
		
		// we can use RegressionEvaluator to check for RMSE
		// see LS page 303
		
		println("completed")
	}
}
