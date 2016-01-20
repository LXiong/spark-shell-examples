/*
  Given the numbers {10, 20, 30, 40, 50} build the sequence of its squares,
  convert each of the items to string and reverse each of the items.
*/

val nums = sc.parallelize(10 to 50 by 10)
val numsSquared = nums.map(num => num * num)

val numsSquaredStr = numsSquared.map(_.toString.reverse)

numsSquaredReversedStr.foreach(item => println(item))

numsSquaredReversedStr.first
numsSquaredReversedStr.top(3)
