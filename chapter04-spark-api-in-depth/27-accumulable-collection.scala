val rdd = sc.parallelize(1 to 10)

import scala.collection.mutable.MutableList
val accumulableCollection = sc.accumulableCollection(MutableList[Int]())

rdd.foreach(x => accumulableCollection += x)
accumulableCollection.value
