val list = List.fill(500)(scala.util.Random.nextInt(10))

/* Step 1: RDD is created with 5 partitions */
val listRDD = sc.parallelize(list, 5)

/* Step 2: Narrow dependency (no shuffling); one-to-one */
val pairsRDD = listRDD.map(x => (x, x * x))
pairsRDD.collect

/*
  Step 3: Wide dependency (suffling required)

  Shuffling operations are expensive, because results must be saved to disk, and also
  data must be sent over the network.

*/
val reducedRDD = pairsRDD.reduceByKey((val1, val2) => val1 + val2)
reducedRDD.collect
reducedRDD.partitions.size

/* Step 4: Narrow dependency (one-to-one)*/
val finalRDD = reducedRDD.mapPartitions(iter => iter.map({case(k, v) => "K=" + k + ", V=" + v }))
finalRDD.collect

println(finalRDD.toDebugString)

/*
  RESULT:
  (5) MapPartitionsRDD[294] at mapPartitions at ...
 |  ShuffledRDD[293] at reduceByKey at ...
 +-(5) MapPartitionsRDD[292] at map at ...
    |  ParallelCollectionRDD[291] at parallelize at ...
*/
