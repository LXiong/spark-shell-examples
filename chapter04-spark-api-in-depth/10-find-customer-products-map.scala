val trxFileName = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter04-spark-api-in-depth/data/ch04_data_transactions.txt"
val trxFileLines = sc.textFile("file://" + trxFileName)
val trxFieldsData = trxFileLines.map(_.split("#"))
val trxByCust = trxFieldsData.map(trxFields => (trxFields(2), trxFields))

/*
  Find the products each customer purchased
*/
val prodsByCust = trxByCust.aggregateByKey(List[String]())(
  (prods, tran) => prods ::: List(tran(3)),
  (prods1, prods2) => prods1 ::: prods2
)

prodsByCust.collect
