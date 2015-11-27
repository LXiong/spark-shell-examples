/*
  The following three vectors contain the same elements.
*/

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

def toBreezeVector(v: Vector) : BV[Double] = v match {
  case dv:DenseVector => new BDV(dv.values)
  case sv:SparseVector => new BSV(sv.indices, sv.values, sv.size)
}

val denseVector1:Vector = Vectors.dense(5., 6., 7., 8.)
val denseVector2:Vector = Vectors.dense(Array(5., 6., 7., 8.))

toBreezeVector(denseVector1) + toBreezeVector(denseVector2)
toBreezeVector(denseVector1).dot(toBreezeVector(denseVector2))
