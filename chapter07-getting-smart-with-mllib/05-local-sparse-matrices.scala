import org.apache.spark.mllib.linalg.{Matrices, Matrix, DenseMatrix, SparseMatrix}

/*
    M = [ 5 0 1
          0 3 4 ]
*/

/* create a sparse matrix */
val sparseMatrix:Matrix = Matrices.sparse(2, 3, Array(0, 1, 2, 4), Array(0, 1, 0, 1), Array(5., 3., 1., 4))

/* convert to DenseMatrix */
sparseMatrix.asInstanceOf[SparseMatrix].toDense

/* create a dense matrix */
val denseMatrix:Matrix = Matrices.dense(2, 3, Array(5., 0., 0., 3., 1., 4.))

/* convert to SparseMatrix */
denseMatrix.asInstanceOf[DenseMatrix].toSparse
