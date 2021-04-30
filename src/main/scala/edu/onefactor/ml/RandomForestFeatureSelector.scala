package edu.onefactor.ml

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SparkSession }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }

object RandomForestFeatureSelector {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val tds = spark.read
      .parquet("/shared/artemy.gusev/AAP-2603/tinkoff_sample/completed_v4/short_pipeline_test_v2")
      .where(col("segment") === "train")

    tds.cache()

    val features = tds.schema("selected")
      .metadata
      .getMetadata("ml_attr")
      .getMetadata("attrs")
      .getMetadataArray("numeric")
      .map(meta => (meta.getLong("idx"), meta.getString("name")))

    val iterations = if (args.length > 0) args(0).toInt else 1

    val averageImportances = calculateAverageFeatureImportances(tds, 42L, iterations)

    val featuresImportance = features
      .map(_._2)
      .zip(averageImportances)
      .map {
        case (feature, importance) => Row(feature, importance)
      }

    val featuresImportanceRdd = spark.sparkContext.parallelize(featuresImportance)

    val featureImportanceSchema = StructType(Seq(
      StructField("feature_name", StringType, false),
      StructField("feature_importance", DoubleType, false)))

    val time = System.currentTimeMillis

    spark.createDataFrame(featuresImportanceRdd, featureImportanceSchema)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/shared/andrey.smirnov/feature_importances/" + time + "_" + iterations)
  }

  private def calculateAverageFeatureImportances(tds: DataFrame, seed: Long, iterations: Int = 1): Array[Double] = {
    val importances = for {
      _ <- 0 until iterations
    } yield calculateFeatureImportances(tds, seed)

    val sumImportances = importances.reduce { (left, right) =>
      left.zip(right).map {
        case ((l, r)) => l + r
      }
    }

    sumImportances.map(_ / iterations)
  }

  private def calculateFeatureImportances(tds: DataFrame, seed: Long): Array[Double] = {
    val randomForest = new RandomForestClassifier()
      .setLabelCol("default_flag")
      .setFeaturesCol("selected")
      .setNumTrees(40)
      .setMaxBins(40)
      .setMaxDepth(5)
      .setFeatureSubsetStrategy("all")
      .setSeed(seed)

    val randomForestModel = randomForest.fit(tds)

    randomForestModel.featureImportances.toArray
  }
}
