val list = List.fill(500)(scala.util.Random.nextInt(100))
val rdd = sc.parallelize(list, 30)
rdd.count
rdd.first

val glommed = rdd.glom
glommed.count
glommed.first
