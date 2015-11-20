implicit object AvgAccumulableParam extends org.apache.spark.AccumulableParam[(Int, Int), Int] {
  def zero(v:(Int, Int)) = (0, 0)
  def addInPlace(v1:(Int, Int), v2:(Int, Int)) = (v1._1 + v2._1, v1._2 + v2._2)
  def addAccumulator(v1:(Int, Int), v2:Int) = (v1._1 + 1, v1._2 + v2)
}


val rdd = sc.parallelize(List(1, 2, 3))
val acc = sc.accumulable((0, 0))
rdd.foreach(x => acc += x)
val mean = acc.value._2.toDouble / acc.value._1
