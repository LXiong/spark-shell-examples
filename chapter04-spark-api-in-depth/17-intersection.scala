val oddNumbers = sc.parallelize(List(1, 3, 5, 7, 9, 11, 13, 15, 17, 19))
val primeNumbers = sc.parallelize(List(2, 3, 5, 7, 11, 13, 17, 19))

val oddPrimeIntersection = oddNumbers.intersection(primeNumbers)
oddPrimeIntersection.collect
