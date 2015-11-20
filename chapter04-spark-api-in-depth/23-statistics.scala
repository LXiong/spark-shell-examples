val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))


val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))
val trxByProduct = trxByCust.map(trxByCustTuple => (trxByCustTuple._2(3), trxByCustTuple._2))

val totalsByProduct = trxByProduct.mapValues(trx => trx(5).toDouble).reduceByKey{case(tot1, tot2) => tot1 + tot2}

val productsFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_products.txt"
val productsFileLines = sc.textFile("file://" + productsFileName)
val productsFieldsData = productsFileLines.map(_.split("#"))

val products = productsFieldsData.map(productFieldsItem => (productFieldsItem(0), productFieldsItem))
val productTotalsAndAttributes = totalsByProduct.join(products);

/*
  S
*/
import scala.math.{min, max}

def createCombiner = (t: Array[String]) => {
  val total = t(5).toDouble
  val qty = t(4).toInt
  (total/qty, total/qty, qty, total)
}

def mergeValue : ((Double, Double, Int, Double), Array[String]) => (Double, Double, Int, Double) = {
  case((mn, mx, qty, tot), t) => {
    val total = t(5).toDouble
    val q = t(4).toInt
    (min(mn, total/q), max(mx, total/q), qty+q, tot+total)
  }
}

def mergeCombiner : ((Double, Double, Int, Double), (Double, Double, Int, Double)) => (Double, Double, Int, Double) = {
  case ((mn1, mx1, q1, tot1), (mn2, mx2, q2, tot2)) => (min(mn1, mn2), max(mx1, mx2), q1+q2, tot1+tot2)
}

val avgByCust = trxByCust.combineByKey(createCombiner,
    mergeValue,
    mergeCombiner,
    new org.apache.spark.HashPartitioner(trxByCust.partitions.size)
  ).mapValues({case(mn, mx, qty, tot) => (mn, mx, qty, tot, tot/qty)})

avgByCust.first
