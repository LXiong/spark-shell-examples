/*
  Classify into even and odd sets the numbers found from 1 to 10.
  Define named functions `isEven` and `isOdd` for the filtering
*/

val nums = sc.parallelize(1 to 10)

def isEven(num : Int) = {
  num % 2 == 0
}

val evenNums = nums.filter(isEven)
evenNums.foreach(num => println(num) + " is an even number")

def isOddLambda = (num : Int) => num % 2 != 0
val oddNums = nums.filter(isOddLambda)
oddNums.foreach(num => println(num) + " is an odd number")
