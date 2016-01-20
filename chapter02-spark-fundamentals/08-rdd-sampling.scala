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

/*
  Obtain a random sampling of 7% of the clients who placed purchases last week
*/

val randomSample = uniqueClientIds.sample(false, 0.07)
randomSample.collect

/*
  Obtain a random sampling of exactly three clients from the clients who placed
  purchases last week
*/
val threeRandomClients = uniqueClientIds.takeSample(false, 3)

/*
  Obtain the first five clients
*/
val fiveFirstClients = uniqueClientIds.take(5);

/*
  Obtain the min and max client ids
*/
uniqueClientIds.min
uniqueClientIds.max
