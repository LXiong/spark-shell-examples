val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))


val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))
val trxByProduct = trxByCust.map(trxByCustTuple => (trxByCustTuple._2(3), trxByCustTuple._2))

/*
  Find totals by Product
*/
val totalsByProduct = trxByProduct.mapValues(trx => trx(5).toDouble).reduceByKey{case(tot1, tot2) => tot1 + tot2}
