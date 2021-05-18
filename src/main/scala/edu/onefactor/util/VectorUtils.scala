package edu.onefactor.util

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors, Vector => MLVector}

import scala.collection.mutable

object VectorUtils {
  def compress(features: MLVector, filterIndices: Array[Int], sortedIndices: Boolean = true): MLVector = features match {
    case SparseVector(_, indices, values) =>
      val newSize = filterIndices.length
      val newValues = new mutable.ArrayBuilder.ofDouble
      val newIndices = new mutable.ArrayBuilder.ofInt
      var i = 0
      var j = 0
      var indicesIdx = 0
      var filterIndicesIdx = 0
      while (i < indices.length && j < filterIndices.length) {
        indicesIdx = indices(i)
        filterIndicesIdx = filterIndices(j)
        if (indicesIdx == filterIndicesIdx) {
          newIndices += j
          newValues += values(i)
          j += 1
          if (sortedIndices) i += 1 else i = 0
        } else if (indicesIdx > filterIndicesIdx) {
          j += 1
          if (!sortedIndices) i = 0
        } else i += 1
      }
      Vectors.sparse(newSize, newIndices.result(), newValues.result())
    case DenseVector(values) =>
      Vectors.dense(filterIndices.map(i => values(i)))
    case skip_null_values =>
      skip_null_values
  }

  def compress2(features: MLVector, filterIndices: Array[Int], sortedIndices: Boolean = true): MLVector = features match {
    case SparseVector(_, indices, values) => compressSparseVector(indices, values, filterIndices)
    case DenseVector(_) => ???
  }

  private def compressSparseVector(indices: Array[Int], values: Array[Double], filterIndices: Array[Int]): MLVector = {
    val valueByIndex = indices.zip(values).toMap

    val vectorElements = filterIndices.zipWithIndex
      .foldLeft(new mutable.ArrayBuffer[(Int, Double)](filterIndices.length)) { case (elements, (vectorIndex, index)) =>
        valueByIndex.get(vectorIndex) match {
          case Some(value) => elements += (index -> value)
          case None => elements
        }
    }

    Vectors.sparse(filterIndices.length, vectorElements)
  }
}
