package edu.onefactor.ml

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RandomForestClassificationModelTrainer {
  private val seed = 42

  private val RootPath = "/shared/andrey.smirnov/random_forest_classifier"

  private val ClassifierModelPath = RootPath + "/model"

  private val ScoredTdsPath = RootPath + "/scored_tds"

  def main(args: Array[String]): Unit = {
    println("random forest classification model training started")

    val spark = SparkSession.builder().getOrCreate()

    val tds = spark.read
      .parquet("/shared/artemy.gusev/AAP-2603/tinkoff_sample/completed_v4/short_pipeline_test_v2")
      .where(col("segment") === "train")

    tds.cache()

    val randomForest = new RandomForestClassifier()
      .setLabelCol("default_flag")
      .setFeaturesCol("selected")
      .setNumTrees(40)
      .setMaxBins(40)
      .setMaxDepth(5)
      .setFeatureSubsetStrategy("all")
      .setSeed(seed)

    val randomForestModel = randomForest.fit(tds)

    val runNumber = args(0)

    randomForestModel.write
      .overwrite()
      .save(ClassifierModelPath + "_" + runNumber)

    val scoredTds = randomForestModel.transform(tds)

    showDataset("dataset scored using trained model", scoredTds) { df =>
      val tdf = scoredDatasetStats(df)

      tdf.orderBy("mapping_id")
    }

    scoredTds.write
      .mode("overwrite")
      .parquet(ScoredTdsPath + "_" + runNumber)
  }

  private def scoredDatasetStats(df: DataFrame): DataFrame = {
    val toArray = udf { vector: MLVector =>
      vector.toArray
    }

    df
      .select(
        col("mapping_id"),
        col("msisdn_hash"),
        col("default_flag"),
        toArray(col("rawPrediction")).as("rawPrediction"),
        toArray(col("probability")).as("probability"),
        col("prediction"))
      .withColumn("rawPredictionSum", expr("rawPrediction[0] + rawPrediction[1]"))
      .withColumn("probabilitySum", expr("probability[0] + probability[1]"))
  }

  private def showDataset(name: String, df: DataFrame, numRows: Int = 20)
                         (tf: DataFrame => DataFrame = identity): Unit = {

    val tdf = tf(df)

    println(s"dataset: $name with schema: ${tdf.schema}")

    tdf.show(numRows = numRows, truncate = false)
  }
}
