val list = List.fill(500)(scala.util.Random.nextInt(10))
val listrdd = sc.parallelize(list, 5)
val pairs = listrdd.map(x => (x, x * x))
val reduced = pairs.reduceByKey((v1, v2) => (v1 + v2))
val finalrdd = reduced.mapPartitions(iter => iter.map({case(k, v) => "(K=" + k + ", V=" + v + ")"}))
finalrdd.collect
println(finalrdd.toDebugString)
