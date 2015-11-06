val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))

/*
    Obtain the number of purchases per client
*/
trxByCust.countByKey

trxByCust.countByKey.map(_._2).sum

// Now it's easy to find the customer who bought the greatest number of products
val trxByCustSortedByNumPurchases = trxByCust.countByKey.toSeq.sortBy(_._2)
val (custID, numPurchases) = trxByCustSortedByNumPurchases.last
