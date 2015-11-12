val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))

/*
  Add the complementary gifts:
    + productID 4 to the client who bought the greatest number of products
    + productID 63 to the client who spent the most
*/
val complementaryTrx = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))
complementaryTrx = complementaryTrx :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")

trxByCust = trxByCust.union(sc.parallelize(complementaryTrx).map(t => (t(2), t)))

val processedTrxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.processed.txt";
trxByCust.map(t => t._2.mkString("#")).saveAsTextFile("file://" + processedTrxFileName)
