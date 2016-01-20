# Spark in Action Examples: Chapter 2 &mdash; Spark Fundamentals
> Scala examples for Spark shell

### 01-count-bsd-hits-in-license
The simplest spark-shell program that can be used to verify your Spark installation. The scripts loads Sparks license text file and counts how many lines contain the string `BSD`.

### 02-rdd-filter-even-nums
Illustrates the simplest use of the `RDD.filter` method to find the even numbers in the sequence of numbers 1..10.
The example also introduces the use of `parallelize` to build an `RDD` of integer numbers:
```scala
  val nums = sc.parallelize(1 to 10) // -> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
```

### 03-rdd-filter-nums-named-functions
Illustrates how to define an implement simple named functions in Scala that are used as arguments for the `filter` method. Two different styles are used:
+ regular functions
+ lambdas

### 04-rdd-map-squares
Illustrates how to use the `map` method to transform an `RDD`. In the example, an RDD of integers is transformed into another RDD of integers by computing their squares.
The example also illustrates the use of `parallelize` with a step value:
```scala
  val nums = sc.parallelize(10 to 50 by 10) // -> {10, 20, 30, 40, 50}
```
### 05-rdd-map-squares-rev-str
Illustrates how to use the `map` method to transform an `RDD[Int]` into an `RDD[String]`. In the example, we compute the squares of the given RDD items, transform its items into strings and then reverse them.

### 06-rdd-map-squares-rev-str-placeholder
The same exercise found in [05-rdd-map-squares-rev-str](###05-rdd-map-squares-rev-str) but using the placeholder syntax:
```scala
  val nums = numsSquares.map(_.toString.reverse)
```
The example also introduces the `first` and `top` methods of RDDs which can be used to obtain the first element and an ordered array of the k largest elements from an RDD.

### 07-rdd-distinct-buying-clients
Illustrates the `distinct` method to remove the duplicates from an RDD. In the example, given last week's transaction log that includes 7 lines (one per day) with each line being a comma-separated list of the client-ids which made a purchase, it is found how many clients bought at least one product during that week.
The example also illustrates the use of `flatMap` method that is used in situations where the result of a transformation yields multiple collections (or arrays) and we are interested in obtaining an `RDD` of the collection elements.

For example:
```scala
val csvLines = sc.textFile("...") // RDD[String] = {"1,2,3","4,5,6","7,8,9"}
val lines = csvLines.map(nums => nums.split(",")) // RDD[Array[String]] = {Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9)}
val nums = csvLines.flatMap(nums => nums.split(",")) // RDD[String] = {1, 2, 3,4, 5, 6,7, 8, 9)}
```

The example also introduces `collect` which collects the contents of an RDD and return an array, and also Scala's `mkString` method to create a String representation from the contents of a Scala array.

### 008-rdd-sampling
Illustrates the following sampling methods on RDDs:
+ `sample` &mdash; lets you obtain a random sample of an RDD with each element having a given probability of being selected. This method returns another RDD.
+ `takeSample` &mdash; lets you obtain a random sample of an RDD with an exact number of elements. This method returns an Array.
+ `sample` &mdash; obtains an array with the first n elements of an RDD. This method also returns an Array.

The example also illustrates the usage of `min` and `max` methods on RDDs.
