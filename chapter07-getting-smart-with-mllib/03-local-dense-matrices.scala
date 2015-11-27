/*
  M = [ 5 0 1
        0 3 4]
*/

import org.apache.spark.mllib.linalg.{Matrices, Matrix}

val denseMatrix:Matrix = Matrices.dense(2, 3, Array(5., 0., 0., 3., 1., 4.));
