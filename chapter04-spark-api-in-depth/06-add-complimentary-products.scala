val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))

/*
    Add a complimentary productID = 70 to customers who bought 5 or more dictionaries (productID = 81)
*/
val trxByCustWithGifts = trxByCust.flatMapValues(trxFields => {
  if (trxFields(3).toInt == 81 && trxFields(4).toInt >= 5) {
    val clonedTrxFields = trxFields.clone()
    clonedTrxFields(3) = "70";
    clonedTrxFields(4) = "1";
    clonedTrxFields(5) = "0.00";
    List(trxFields, clonedTrxFields)
  } else {
    List(trxFields)
  }
})
