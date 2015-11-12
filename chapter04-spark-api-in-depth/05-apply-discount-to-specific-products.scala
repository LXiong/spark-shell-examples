val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))

/*
    Apply 5% discount to purchases with 2 or more productID=25
*/
val trxByCustWithDiscount = trxByCust.mapValues(trxFields => {
  if(trxFields(3).toInt == 25 && trxFields(4).toInt >= 2) {
    trxFields(5) = (trxFields(5).toDouble * 0.95).toString
  }
  trxFields
})
