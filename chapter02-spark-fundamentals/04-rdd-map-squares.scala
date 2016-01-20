/*
  Given the numbers {10, 20, 30, 40, 50} build the sequence of its squares.
*/

val nums = sc.parallelize(10 to 50 by 10)
val numsSquared = nums.map(num => num * num)

numsSquared.foreach(item => println(item))
