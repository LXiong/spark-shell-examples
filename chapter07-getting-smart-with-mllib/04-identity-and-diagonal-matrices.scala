import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}

val denseIdentity4x4:Matrix = Matrices.eye(4)
val sparseIdentity4x4:Matrix = Matrices.speye(4)

val ones:Matrix = Matrices.ones(2, 4)
val zeros:Matrix = Matrices.zeros(4, 2)

val v = Vectors.dense(1., 2., 3., 4., 5.)
val diag:Matrix = Matrices.diag(v)
