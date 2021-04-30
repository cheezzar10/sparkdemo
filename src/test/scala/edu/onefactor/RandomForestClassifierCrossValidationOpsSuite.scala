package edu.onefactor

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{ OneHotEncoderEstimator, StringIndexer, VectorAssembler }
import org.apache.spark.ml.linalg.{ Vector => MLVector }
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.{ BeforeAndAfterAll, FunSuite }

class RandomForestClassifierCrossValidationOpsSuite extends FunSuite with BeforeAndAfterAll {
  lazy val spark = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.allowMultipleContexts", "false")
    .getOrCreate()

  test("random forest classifier demo") {
    val trainCategorical = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///Users/andrey.smirnov/Downloads/kaggle-bosch/train_categorical.csv")

    import org.apache.spark.sql.functions._

    showDataset("train categorical w/o nulls", trainCategorical) { df =>
      df.where(col("L0_S22_F545").isNotNull).select("Id", "L0_S22_F545")
    }

    val trainNumeric = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///Users/andrey.smirnov/Downloads/kaggle-bosch/train_numeric.csv")

    // showDataset("train numeric data", trainNumeric)()

    val trainData = trainCategorical
      .join(trainNumeric, "Id")
      .select(
        col("Response").as("label"),
        coalesce(col("L0_S22_F545"), lit("NA")).as("L0_S22_F545"),
        coalesce(col("L0_S0_F0"), lit(0.0)).as("L0_S0_F0"),
        coalesce(col("L0_S0_F2"), lit(0.0)).as("L0_S0_F2"),
        coalesce(col("L0_S0_F4"), lit(0.0)).as("L0_S0_F4"))

    showDataset("train dataset w/o nulls", trainData) { df =>
      df.where(col("label") =!= 0.0)
    }

    val indexer = new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol("L0_S22_F545")
      .setOutputCol("L0_S22_F545Index")

    // val trainDataIndexed = indexer.fit(trainData).transform(trainData)

    // showDataset("train data indexed", trainDataIndexed)(_.where(col("L0_S22_F545") =!= "NA"))

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("L0_S22_F545Index"))
      .setOutputCols(Array("L0_S22_F545Encoded"))

    // val trainDataEncoded = encoder.transform(trainDataIndexed)

    // showDataset("train data encoded", trainDataEncoded)()

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("L0_S22_F545Encoded", "L0_S0_F0", "L0_S0_F2", "L0_S0_F4"))
      .setOutputCol("features")

    // val trainDataVectorized = vectorAssembler.transform(trainDataEncoded)

    // showDataset("train data vectorized", trainDataVectorized)()

    val randomForestClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler, randomForestClassifier))

    // val pipelineModel = trainRandomForestClassifierModelInstance(pipeline, trainData)

    val optimalModel = trainOptimalRandomForestClassifierModel(pipeline, trainData)
  }

  private def trainRandomForestClassifierModelInstance(pipeline: Pipeline, trainData: DataFrame) = {
    val pipelineModel = pipeline.fit(trainData)

    // processing train dataset with model
    val result = pipelineModel.transform(trainData)

    showDataset("result", result) { df =>
      // df.where(col("label") === lit(1))
      df.where(col("prediction") =!= 0.0)
    }

    // displaying model parameters
    for (pipelineModelStage <- pipelineModel.stages) {
      println("pipeline stage: " + pipelineModelStage)
    }

    val randomForestClassifierModel = pipelineModel.stages.last
      .asInstanceOf[RandomForestClassificationModel]

    println(s"random forest classifier model: num trees = ${randomForestClassifierModel.getNumTrees}, " +
      s"feature subset strategy = ${randomForestClassifierModel.getFeatureSubsetStrategy}, " +
      s"impurity = ${randomForestClassifierModel.getImpurity}, " +
      s"max bins = ${randomForestClassifierModel.getMaxBins}, " +
      s"max depth = ${randomForestClassifierModel.getMaxDepth}")

    pipelineModel
  }

  private def trainOptimalRandomForestClassifierModel(pipeline: Pipeline, trainData: DataFrame) = {
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC") // ROC AUC ( area under curve )
      .setRawPredictionCol("prediction")
      .setLabelCol("label")

    val randomForestClassifier = pipeline.getStages.last.asInstanceOf[RandomForestClassifier]

    val randomForestClassifierParams = new ParamGridBuilder()
      .addGrid(randomForestClassifier.numTrees, Array(20))
      .addGrid(randomForestClassifier.featureSubsetStrategy, Array("auto"))
      .addGrid(randomForestClassifier.impurity, Array("gini"))
      .addGrid(randomForestClassifier.maxBins, Array(32))
      .addGrid(randomForestClassifier.maxDepth, Array(7))
      .build()

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(randomForestClassifierParams)
      .setNumFolds(4)

    val crossValidatorModel = crossValidator.fit(trainData)

    // val optimalPipelineModel = crossValidatorModel.bestModel

    val optimalResult = crossValidatorModel.transform(trainData)

    val toArray = udf { vector: MLVector =>
      vector.toArray
    }

    showDataset("best model result", optimalResult) { df =>
      df.where(col("label") =!= 0.0)
        .select(
          col("label"),
          toArray(col("rawPrediction")).as("rawPrediction"),
          toArray(col("probability")).as("probability"),
          col("prediction"))
        .withColumn("rawPredictionSum", expr("rawPrediction[0] + rawPrediction[1]"))
        .withColumn("probabilitySum", expr("probability[0] + probability[1]"))
    }

    crossValidatorModel
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  private def showDataset(
    name: String,
    df: DataFrame,
    numRows: Int = 20)(transformer: DataFrame => DataFrame = identity): Unit = {

    val transformedDataFrame = transformer(df)

    println(s"dataset: $name with schema: ${transformedDataFrame.schema}")

    transformedDataFrame.show(numRows = numRows, truncate = false)
  }
}
