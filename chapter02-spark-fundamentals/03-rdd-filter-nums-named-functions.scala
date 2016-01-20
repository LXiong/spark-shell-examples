/*
  Print the even numbers found from 1 to 10
*/

val nums = sc.parallelize(1 to 10)
val evenNums = nums.filter(num => num % 2 == 0)
evenNums.foreach(num => println(num) + " is an even number")
