/*
  Count the number of lines in Spark's License file with the word BSD on them
*/

/* I assume you've started the shell from $SPARK_HOME */
val pathToLicense = "LICENSE"

val licLines = sc.textFile("LICENSE")
licLines.count

val bsdLines = licLines.filter(line => line.contains("BSD"))
bsdLines.count

bsdLines.foreach(line => println(line))
