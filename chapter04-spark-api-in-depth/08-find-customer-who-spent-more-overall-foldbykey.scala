val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))

/*
    Find the customerID for the customer who spent most overall
*/
val amountSpentByTrx = trxByCust.mapValues(trx => trx(5).toDouble)
val amountSpentByCust = amountSpentByTrx.foldByKey(0)((amt1, amt2) => amt1 + amt2)
amountSpentByCust.collect

amountSpentByCust.collect.toSeq.sortBy(_._2).last
