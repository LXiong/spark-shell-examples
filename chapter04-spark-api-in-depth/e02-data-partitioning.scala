val dishes = Array(
                   Array("pork", 800, "meat"),
                   Array("beef", 700, "meat"),
                   Array("chicken", 400, "meat"),
                   Array("french fries", 530, "other"),
                   Array("rice", 350, "other"),
                   Array("salmon", 450, "fish"))

/* default partitioning: you'll end up with as many partitions as indicated by `spark.default.parallelism` */
val dishesRDD = sc.parallelize(dishes)
dishesRDD.partitions.size

/* using the overloaded version that lets you select the number of partitions */
val dishesRDD = sc.parallelize(dishes, 12)
dishesRDD.partitions.size

/* sc.parallelize does NOT accept a Partitioner */
//ERROR!!! val dishesRDD = sc.parallelize(dishes, new org.apache.spark.HashPartitioner(12)) Found HashPartitioner, expected Int


val dishesByTypeRDD = dishesRDD.map(dish => (dish(2), dish))
dishesByTypeRDD.collect

val totalCaloriesByTypeRDD = dishesByTypeRDD.mapValues(dish => dish(1).asInstanceOf[Int]).reduceByKey((cal1, cal2) => cal1 + cal2)
totalCaloriesByTypeRDD.partitions.size

/* glom is useful while testing to see how data is spread across partitions */
val numbersRDD = sc.parallelize(1 to 10)
numbersRDD.partitions.size
numbersRDD.glom.collect
