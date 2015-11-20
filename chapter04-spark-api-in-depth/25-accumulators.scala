val acc = sc.accumulator(0, "my-accumulator")
val list = sc.parallelize(1 to 1000000)
list.foreach(x => acc += 1)
acc.value

/*
  The following line fails because the value of the accumulator
  can only be queried inside the driver and not in the workers
*/
list.foreach(x => acc.value)
