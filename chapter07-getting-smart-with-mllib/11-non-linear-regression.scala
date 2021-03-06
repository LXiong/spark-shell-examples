val datasetPath = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter07-getting-smart-with-mllib/data/housing-dataset/housing-data.csv"
val housingDatasetLines = sc.textFile(datasetPath, 6)
housingDatasetLines // -> 506 lines

import org.apache.spark.mllib.linalg.{Vectors}
val housingValues = housingDatasetLines.map(line => Vectors.dense(line.split(",").map(_.trim.toDouble)))


// Method 1: through a RowMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
val housingMatrix = new RowMatrix(housingValues)
val housingStatistics = housingMatrix.computeColumnSummaryStatistics()

housingStatistics.min // -> min for each of the columns
housingStatistics.variance // -> variance for each of the columns
housingStatistics.normL1 // -> sum of absolute values per column
housingStatistics.normL2 // -> Euclidean norm


// Method 2: using the Statistics methods
import org.apache.spark.mllib.stat.Statistics
val housingStatisticsAlt = Statistics.colStats(housingValues)
housingStatisticsAlt.min // -> min for each of the columns
housingStatisticsAlt.variance // -> min for each of the columns

val housingColumnSimilarities = housingMatrix.columnSimilarities

val housingCovarianceMatrix = housingMatrix.computeCovariance

import org.apache.spark.mllib.regression.LabeledPoint
val housingData = housingValues.map(x => {
  val a = x.toArray
  LabeledPoint(a(a.length - 1), Vectors.dense(a.slice(0, a.length - 1)))
})

val sets = housingData.randomSplit(Array(0.8, 0.2))
val housingDataTrain = sets(0)
val housingDataTest = sets(1)

import org.apache.spark.mllib.feature.StandardScaler
var scaler = new StandardScaler(true, true).fit(housingDataTrain.map(x => x.features))

var trainingScaled = housingDataTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
var testScaled = housingDataTest.map(x => LabeledPoint(x.label, scaler.transform(x.features)))


// method 1: using the train method
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val model = LinearRegressionWithSGD.train(trainingScaled, 200, 1.0)

// method 2: non-standard way
var alg = new LinearRegressionWithSGD()
alg.setIntercept(true)
alg.optimizer.setNumIterations(200)
trainingScaled.cache
testScaled.cache
val model = alg.run(trainingScaled)

val testPredictions = testScaled.map(x => (model.predict(x.features), x.label))

testPredictions.collect

// compute the root mean squared error
math.sqrt(testPredictions.map{case(p,l) => math.pow(p - 1 , 2)}.mean)

import org.apache.spark.mllib.evaluation.RegressionMetrics
val testMetrics = new RegressionMetrics(testPredictions)
testMetrics.rootMeanSquaredError
testMetrics.meanSquaredError
testMetrics.meanAbsoluteError
testMetrics.r2
testMetrics.explainedVariance

println(model.weights.toArray.map(x => x * x).zipWithIndex.sortBy(_._1).mkString(", "))

/*
   Non-linear regression
 */

import org.apache.spark.mllib.linalg.{Vectors,Vector}
def addHighPols(v:Vector) : Vector = {
  Vectors.dense(v.toArray.flatMap(x => Array(x, x*x)))
}

val housingHP = housingData.map(x => LabeledPoint(x.label, addHighPols(x.features)))

/* Splitting the data */
val setsHP = housingHP.randomSplit(Array(0.8, 0.2))
val housingHPTrain = setsHP(0)
val housingHPTest = setsHP(1)

/* Scale */
val scalerHP = new StandardScaler(true, true).fit(housingHPTrain.map(x => x.features))
val trainHPScaled = housingHPTrain.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
val testHPScaled = housingHPTest.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
trainHPScaled.cache
testHPScaled.cache

/* find adequate step sizes and number of iterations */
import org.apache.spark.rdd.RDD
def iterateLRwSGD(iterNums:Array[Int], stepSizes:Array[Double], train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new LinearRegressionWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val testPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math
            .sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredTest = math
            .sqrt(testPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredTest))
// Uncomment to obtain weights as well
//    println("%d, %4.2f -> %.4f, %.4f (%s, %f)".format(numIter, step, meanSquared,
//                              meanSquaredTest, model.weights, model.intercept))
  }
}

iterateLRwSGD(Array(200, 400), Array(0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5), trainHPScaled, testHPScaled)

/*
  Overfitting: let's check with a higher number of iterations
*/
iterateLRwSGD(Array(10000,15000, 30000, 50000), Array(1.1), trainHPScaled, testHPScaled)

/*
  Avoiding overfitting using Lasso regression
*/
import org.apache.spark.mllib.regression.LassoWithSGD
def iterateLasso(iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes) {
    val alg = new LassoWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step).setRegParam(regParam)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val testPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,1) => math.pow(p-1,2)}).mean())
    val meanSquaredTest = math.sqrt(testPredicts.map({case(p,1) => math.pow(p-1,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredTest))
    println("\tweights: "+model.weights)
  }
}

// this crashes
iterateLasso(Array(200, 400, 1000, 3000, 6000, 10000, 50000, 200000, 300000), Array(1.1), 0.01, trainHPScaled, testHPScaled)

/*
  Avoiding overfitting using Ridge regression
*/
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
def iterateRidge(iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new RidgeRegressionWithSGD()
    alg.setIntercept(true)
    alg.optimizer.setNumIterations(numIter).setRegParam(regParam).setStepSize(step)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val testPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredTest = math.sqrt(testPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredTest))
  }
}

/* this works OK */
iterateRidge(Array(200, 400, 1000, 3000, 6000, 10000, 50000, 200000, 300000), Array(1.1), 0.01, trainHPScaled, testHPScaled)

/*
  Using mini-batch stochastic gradient descent
*/
def iterateLRwSGDBatch(iterNums:Array[Int], stepSizes:Array[Double], fractions:Array[Double], train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes; miniBFraction <- fractions) {
    val alg = new LinearRegressionWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
    alg.optimizer.setMiniBatchFraction(miniBFraction)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val testPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredTest = math.sqrt(testPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f %5.3f -> %.4f, %.4f".format(numIter, step, miniBFraction, meanSquared, meanSquaredTest))
  }
}

/*
  Using LBFGS
*/
import org.apache.spark.mllib.optimization.LBFGS
import org.apache.spark.mllib.optimization.LeastSquaresGradient
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.util.MLUtils
def iterateLBFGS(regParams:Array[Double], numCorrections:Int, tolerance:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  val dimnum = train.first().features.size
  for(regParam <- regParams)
  {
    val (weights:Vector, loss:Array[Double]) = LBFGS.runLBFGS(
      train.map(x => (x.label, MLUtils.appendBias(x.features))),
      new LeastSquaresGradient(),
      new SquaredL2Updater(),
      numCorrections,
      tolerance,
      50000,
      regParam,
      Vectors.zeros(dimnum+1))

    val model = new LinearRegressionModel(
      Vectors.dense(weights.toArray.slice(0, weights.size - 1)),
      weights(weights.size - 1))

    val trainPredicts = train.map(x => (model.predict(x.features), x.label))
    val testPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(trainPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredTest = math.sqrt(testPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%5.3f, %d -> %.4f, %.4f".format(regParam, numCorrections, meanSquared, meanSquaredTest))
  }
}

iterateLBFGS(Array(0.005, 0.007, 0.01, 0.02, 0.03, 0.05, 0.1), 10, 1e-5, trainHPScaled, testHPScaled)
