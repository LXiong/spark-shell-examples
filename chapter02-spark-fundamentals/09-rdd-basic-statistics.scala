val numsRDD = sc.parallelize(1 to 10)

/* calculate the mean, sum, min, max, variance and std deviation using methods */
numsRDD.mean

numsRDD.sum

numsRDD.min
numsRDD.max

numsRDD.variance
numsRDD.stdev

/* use the stats method to obtain all those results in a single method call */
numsRDD.stats


/* Approximate mean, sum, etc. */
val largeNumericRDD = sc.parallelize(1 to 100000000)

/* the first argument is the timeout value, the second one is the confidence */
largeNumericRDD.sumApprox(1000, 0.90)
largeNumericRDD.sumApprox(1500, 0.90)
largeNumericRDD.sumApprox(2500, 0.90)

largeNumericRDD.meanApprox(1000, 0.90)
largeNumericRDD.meanApprox(1500, 0.90)
largeNumericRDD.meanApprox(2500, 0.90)
