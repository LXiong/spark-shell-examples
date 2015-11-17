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
  Find the products that were not sold yesterday
*/
val totalsWithMissingProducts = products.leftOuterJoin(totalsByProduct)
totalsWithMissingProducts.count
totalsWithMissingProducts.first

// or alternatively
val totalsWithMissingProducts2 = totalsByProduct.rightOuterJoin(products)
totalsWithMissingProducts2.count
totalsWithMissingProducts2.first

// we should prefer the first option because products contains more data
val missingProds = totalsWithMissingProducts.filter(item => item._2._2 == None).map(_._2._1)


// we can also use `subtractByKey`
val totalsWithMissingProducts3 = products.subtractByKey(totalsByProduct)
totalsWithMissingProducts3.count
totalsWithMissingProducts3.first

// or even cogroup
val prodTotalsCogroup = totalsByProduct.cogroup(products)
val missingProds = prodTotalsCogroup.filter(item => item._2._1.isEmpty).map(item => item._2._2)
missingProds.count
