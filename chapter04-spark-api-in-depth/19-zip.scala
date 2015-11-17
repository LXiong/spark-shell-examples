val rdd1 = sc.parallelize(List('a', 'b', 'c'))
val rdd2 = sc.parallelize(List(1, 2, 3))

val zippedResult = rdd1.zip(rdd2)
zippedResult.collect
