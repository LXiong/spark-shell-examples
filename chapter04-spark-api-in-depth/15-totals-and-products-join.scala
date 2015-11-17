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

/*
  Find the result of joining the totalsByProduct with the products:
    Left = (productID, totalAmount)   Right = (productID, productFields)
                    join = (productID, (totalAmount, productFields))
*/

val productTotalsAndAttributes = totalsByProduct.join(products);
productTotalsAndAttributes.first;

// You can also obtain the same using cogroup
val productTotalsAndAttributes = totalsByProduct.cogroup(products);
productTotalsAndAttributes.first;
