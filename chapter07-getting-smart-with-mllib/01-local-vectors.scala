/*
  The following three vectors contain the same elements.
*/

import org.apache.spark.mllib.linalg.{Vectors, Vector}

val denseVector1:Vector = Vectors.dense(5., 6., 7., 8.)
val denseVector2:Vector = Vectors.dense(Array(5., 6., 7., 8.))
val sparseVector:Vector = Vectors.sparse(4, Array(0, 1, 2, 3), Array(5., 6., 7., 8.))

denseVector1.size     // -> 4
denseVector2(2)       // -> 7.0
denseVector2.toArray  // -> Array(5.0, 6.0, 7.0, 8.0)
sparseVector.toArray  // -> Array(5.0, 6.0, 7.0, 8.0)
