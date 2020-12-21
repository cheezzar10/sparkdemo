package edu.onefactor

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types.{ DataType, ArrayType, StringType }
import org.apache.spark.ml.param.{ IntParam, ParamValidators }

class MyTokenizer(override val uid: String, mw: Int = 0) extends UnaryTransformer[String, Seq[String], MyTokenizer] {
	val maxWords: IntParam = new IntParam(this, "maxWords", "The maximum number of words to return", ParamValidators.gtEq(0))
	
	set(maxWords, mw)

	protected def createTransformFunc: String => Seq[String] = _.split("\\s").take($(maxWords))

	protected def outputDataType: DataType = new ArrayType(StringType, true)
}