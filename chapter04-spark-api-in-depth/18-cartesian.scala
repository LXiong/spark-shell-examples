val rdd1 = sc.parallelize(List('a', 'b', 'c'))
val rdd2 = sc.parallelize(List(1, 2))

val cartesianProduct = rdd1.cartesian(rdd2)
cartesianProduct.collect

/*
  Obtain all the pairs that are divisible
*/
val rdd1 = sc.parallelize(List(7, 8, 9))
val rdd2 = sc.parallelize(List(1, 2, 3))

val cartesianProduct = rdd1.cartesian(rdd2).collect
val divisiblePairs = cartesianProduct.filter(tuple2 => tuple2._1 % tuple2._2 == 0)
