val logFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter02-spark-fundamentals/data/client-ids.log"

val purchasesLines = sc.textFile(logFilename)

val purchasingClients = purchasesLines.flatMap(line => line.split(","))

val clientIds = purchasingClients.map(_.toInt)
val uniqueClientIds = clientIds.distinct

uniqueClientIds.collect

/*
  Obtain the histogram for intervals:
    + 1  <= bucket_1 < 50
    + 50 <= bucket_2 <= 100
*/
uniqueClientIds.histogram(Array(1.0, 50.0, 100.0))

/* verifications */
uniqueClientIds.filter(id => (id >= 1) && (id < 50)).count
uniqueClientIds.filter(id => (id >= 50) && (id <= 100)).count // note the `<=` in the last interval


/*
  Obtain the histogram for 3 intervals
*/
uniqueClientIds.histogram(3) // -> [2, 34, 66, 98], [13, 8, 8]

/* verifications */
uniqueClientIds.filter(id => (id >= 2) && (id < 34)).count   // -> 13
uniqueClientIds.filter(id => (id >= 34) && (id < 66)).count  // -> 8
uniqueClientIds.filter(id => (id >= 66) && (id <= 98)).count // -> 8 * note the `<=` in the last interval
