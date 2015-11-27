/*
  Step 1: Load and parse the data

  Objective: obtain an RDD of Vectors with the dataset info, one vector per line.
*/
val datasetPath = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter07-getting-smart-with-mllib/data/housing-dataset/housing-data.csv"
val housingDatasetLines = sc.textFile(datasetPath, 6)
housingDatasetLines // -> 506 lines

import org.apache.spark.mllib.linalg.{Vectors}
val housingValues = housingDatasetLines.map(line => Vectors.dense(line.split(",").map(_.trim.toDouble)))

/*
  Step 2: Analyze and familiarize yourself with the data

  Objective: obtain the multivariate statistical summary.
*/


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

/*
  Step 3: Obtain column cosine similarities
*/
val housingColumnSimilarities = housingMatrix.columnSimilarities

/*
  Step 4: Obtain the covariance matrix
*/
val housingCovarianceMatrix = housingMatrix.computeCovariance

/*
  Step 4: Confirm to labeled points
*/
import org.apache.spark.mllib.regression.LabeledPoint
val housingData = housingValues.map(x => {
  val a = x.toArray
  LabeledPoint(a(a.length - 1), Vectors.dense(a.slice(0, a.length - 1)))
})

/*
  Step 5: Split the dataset into training and test using a 80% - 20% ratio
*/
val sets = housingData.randomSplit(Array(0.8, 0.2))
val housingDataTrain = sets(0)
val housingDataTest = sets(1)

/*
  Step 6: Feature scaling and mean normalization
*/
import org.apache.spark.mllib.feature.StandardScaler
var scaler = new StandardScaler(true, true).fit(housingDataTrain.map(x => x.features))

var trainingScaled = housingDataTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
var testScaled = housingDataTest.map(x => LabeledPoint(x.label, scaler.transform(x.features)))

/*
  Step 7: Fitting the Linear Regression Model
*/

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

/*
  Step 8: Predicting target values and comparing with test data sets
*/
val testPredictions = testScaled.map(x => (model.predict(x.features), x.label))

testPredictions.collect

// compute the root mean squared error
math.sqrt(testPredictions.map{case(p,l) => math.pow(p - 1 , 2)}.mean)

/*
  Step 9: Evaluating the model's performance with RegressionMetrics
*/
import org.apache.spark.mllib.evaluation.RegressionMetrics
val testMetrics = new RegressionMetrics(testPredictions)
testMetrics.rootMeanSquaredError
testMetrics.meanSquaredError
testMetrics.meanAbsoluteError
testMetrics.r2
testMetrics.explainedVariance

/*
  Step 10: Inspecting the weights of the model
*/
println(model.weights.toArray.map(x => x * x).zipWithIndex.sortBy(_._1).mkString(", "))

/*
  Step 11: Saving the model, so that it can be used later on without reconstructing
*/
model.save(sc, "file:///home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter07-getting-smart-with-mllib/data/housing-dataset/model")
