/*
  Given a log file with contains the clientIds of the customers who placed
  purchases last week, find how many different clients bought at least one
  product during that week.

  The file contains seven lines (one per day), each line being a CSV of clientIds
  who placed purchases.
*/

val logFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter02-spark-fundamentals/data/client-ids.log"

val purchasesLines = sc.textFile(logFilename)
purchasesLines.count // -> must be 7

/* map is not useful here, as we obtain an RDD[Array[String]] with the clients */
val purchasingClientsPerDay = purchasesLines.map(line => line.split(","))
purchasingClientsPerDay.collect

/* flatMap */
val purchasingClients = purchasesLines.flatMap(line => line.split(","))
purchasingClients.collect

purchasingClients.collect.mkString(";")

val clientIds = purchasingClients.map(_.toInt)
val uniqueClientIds = clientIds.distinct
println("Number of different clients who placed a purchase last week: " + uniqueClientIds.count)
println("Number of purchases placed last week: " + purchasingClients.count)
