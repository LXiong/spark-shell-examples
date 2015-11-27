import org.apache.spark.mllib.linalg.{Matrices, Matrix, DenseMatrix, SparseMatrix}

/*
    M = [ 5 0 1
          0 3 4 ]
*/

val dm:Matrix = Matrices.dense(2, 3, Array(5., 0., 0., 3., 1., 4.));

dm(1, 1) // -> 3

dm.transpose
