package edu.onefactor.util

import org.apache.spark.ml.linalg.{Vectors, Vector => MLVector}
import org.openjdk.jmh.annotations.Mode._
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit
import scala.util.Random

import scala.collection.mutable

@BenchmarkMode(Array(Throughput))
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, warmups = 1, jvmArgs = Array("-Xms1g", "-Xmx1g"))
class VectorUtilsBenchmark {
  import VectorUtilsBenchmark._

  @Benchmark
  def compressSparseVectorUsingLowLevelAlgorithm(state: VectorUtilsBenchmarkState): MLVector = {
    VectorUtils.compress(state.vector, state.selectedIndexes, false)
  }

  @Benchmark
  def compressSparseVectorUsingHighLevelAlgorithm(state: VectorUtilsBenchmarkState): MLVector = {
    VectorUtils.compress2(state.vector, state.selectedIndexes, false)
  }
}

object VectorUtilsBenchmark {
  @State(Scope.Benchmark)
  class VectorUtilsBenchmarkState {
    var vector: MLVector = _

    var selectedIndexes: Array[Int] = _

    @Param(Array("100", "1000"))
    var vectorSize: Int = 0

    @Setup(Level.Trial)
    def setup(): Unit = {
      vector = randomSparseVector(vectorSize, 0.2)

      val activeIndexes = mutable.ArrayBuffer.empty[Int]
      vector.foreachActive {
        case (index, _) => activeIndexes += index
      }

      selectedIndexes = Random.shuffle(activeIndexes)
        .take(vector.numActives / 2)
        .toArray
    }
  }

  private def randomSparseVector(size: Int, fillRatio: Double) = {
    val vectorIndexes = Random.shuffle((0 until size).toVector)

    val vectorValues = vectorIndexes.take((size * fillRatio).toInt)
      .map(index => (index, Random.nextDouble()))

    // println("random sparse vector values: " + vectorValues)

    Vectors.sparse(size, vectorValues)
  }
}
