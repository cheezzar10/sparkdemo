package edu.onefactor

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline

object Pipelines {
	def main(args: Array[String]) = {
		val spark = SparkSession.builder()
				.getOrCreate()

		val dataPath = "/Volumes/Data/Users/andrey/Documents/Scala/3rdparty/spark-def-guide"
		val df = spark.read.load(dataPath + "/data/regression")

		val lr = new LinearRegression
		
		// pipeline can be trained ( by calling fit ) as a whole
		// here we can use additional stage, for example vectorization transformer
		// it's missing here cause data is already in vectorized form
		val pl = new Pipeline().setStages(Array(lr))
		
		// now we have a trained model expressed as a pipeline
		// TODO use test dataset here ( use randomSplit here )
		val plm = pl.fit(df)
		
		// performing actual prediction here
		val rdf = plm.transform(df)
		
		// actually linear regression can be trained separate from pipeline
		// and trained parameters can be printed using the following methods

		// lrm.summary
		// summary.residuals.show()
		// summary.objectiveHistory
		// summary.rootMeanSquaredError
		// summary.r2
		
		println("completed")
	}
}