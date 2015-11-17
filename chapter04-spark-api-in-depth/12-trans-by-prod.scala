val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))


/*
  Find transactions by product
*/
val trxByProduct = trxFieldsData.map(trxFields => (trxFields(3), trxFields))

trxByProduct.first
trxByProduct.collect

/*
 Alternatively, you can build the map from the trxByCust
*/
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))
val trxByProduct = trxByCust.map(trxByCustTuple => (trxByCustTuple._2(3), trxByCustTuple._2))
trxByProduct.first
trxByProduct.collect
