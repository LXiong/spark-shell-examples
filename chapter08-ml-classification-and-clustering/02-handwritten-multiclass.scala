/*
  Step 1: Load and parse the data
*/
val datasetPath = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter08-ml-classification-and-clustering/data/handwritten-dataset/penbased.dat"
val penDatasetLines = sc.textFile(datasetPath, 4)
penDatasetLines.count // -> 9912 lines

// Convert numeric to Double
val penValues = penDatasetLines.map(x => x.split(", ")).map(row => row.map(x => x.toDouble))


// Load it into a dataframe with the following schema
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
val penSchema = StructType(Array(
  StructField("pix1", DoubleType, true),
  StructField("pix2", DoubleType, true),
  StructField("pix3", DoubleType, true),
  StructField("pix4", DoubleType, true),
  StructField("pix5", DoubleType, true),
  StructField("pix6", DoubleType, true),
  StructField("pix7", DoubleType, true),
  StructField("pix8", DoubleType, true),
  StructField("pix9", DoubleType, true),
  StructField("pix10", DoubleType, true),
  StructField("pix11", DoubleType, true),
  StructField("pix12", DoubleType, true),
  StructField("pix13", DoubleType, true),
  StructField("pix14", DoubleType, true),
  StructField("pix15", DoubleType, true),
  StructField("pix16", DoubleType, true),
  StructField("label", DoubleType, true)
))

import sqlContext.implicits._
import org.apache.spark.sql.Row
val dfpen = sqlContext.applySchema(penValues.map(Row.fromSeq(_)), penSchema)
dfpen.show

// Assembling columns as required
import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(dfpen.columns.diff(Array("label")))
val penlpoints = va.transform(dfpen).select("features", "label")

/*
  Fitting the model
*/

// Splitting the data into training and testing data setInputCols
val pensets = penlpoints.randomSplit(Array(0.8, 0.2))
val penTrain = pensets(0).cache
val penTest = pensets(1).cache

penTrain.count
penTest.count

// Using a logistic regression classifier
import org.apache.spark.ml.classification.LogisticRegression
val penlr = new LogisticRegression().setRegParam(0.01)

import org.apache.spark.ml.classification.OneVsRest
val ovrest = new OneVsRest()
ovrest.setClassifier(penlr)

val ovrestmodel = ovrest.fit(penTrain)

// Compute predictions from the testing data set
val penresult = ovrestmodel.transform(penTest)

// Convert to RDD to use MLlib's MulticlassMetrics
val penPreds = penresult.select("prediction", "label").map(row => (row.getDouble(0), row.getDouble(1)))

// Construct the MulticlassMetrics object
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val penmm = new MulticlassMetrics(penPreds)

// Evaluation of the model
penmm.precision(3)
penmm.recall(3)
penmm.fMeasure(3)

// Obtaining the confusion matrix
// (i,j) = number of elements from the i class that were classified as j class
penmm.confusionMatrix
