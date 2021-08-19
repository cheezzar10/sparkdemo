package edu.onefactor

import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, FlatSpec }

class JoinDatasetsSpec extends FlatSpec with BeforeAndAfterAll {
  lazy val spark = SparkSession.builder()
    .appName("join datasets demo")
    .master("local[*]")
    .getOrCreate()

  override def beforeAll(): Unit = {
    val applicationId = spark.sparkContext.applicationId

    println(s"spark context started - application id: $applicationId")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  it should "join datasets" in {
    import spark.implicits._

    val sample = Seq(
      ("id1", 0.0),
      ("id2", 1.0),
      ("id3", 1.0)).toDF("id", "target")

    val features = Seq(
      ("id1", 0.2, 0.3),
      ("id2", 1.1, 1.2),
      ("id3", 2.3, 2.4)).toDF("id", "feature1", "feature2")

    val joined = sample.join(features, IndexedSeq("id"))

    joined.show()
  }
}
