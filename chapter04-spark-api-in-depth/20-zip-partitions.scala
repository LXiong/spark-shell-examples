val rdd1 = sc.parallelize(1 to 10, 10)
val rdd2 = sc.parallelize((1 to 8).map(x => "n" + x), 10)

rdd1.collect
rdd2.collect

val zipPartitionsResult = rdd1.zipPartitions(rdd2, true)((iter1, iter2) => {
  iter1.zipAll(iter2, -1, "empty").map({case(x1, x2) => x1 + "-" + x2})
}).collect
