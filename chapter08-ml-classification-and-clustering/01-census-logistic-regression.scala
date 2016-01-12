/*
  Step 1: Load and parse the data
*/
val datasetPath = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter08-ml-classification-and-clustering/data/census-dataset/adult-mod.csv"
val censusDatasetLines = sc.textFile(datasetPath, 6)
censusDatasetLines.count // -> 48842 lines

// Convert numeric to Double, otherwise keep type as it is
val censusValues = censusDatasetLines.map(x => x.split(", ")).map(row => row.map(x => try { x.toDouble } catch { case _ : Throwable => x }))

// Load it into a DataFrame
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
val adultSchema = StructType(Array(
  StructField("age", DoubleType, true),
  StructField("workclass", StringType, true),
  StructField("fnlwgt", DoubleType, true),
  StructField("education", StringType, true),
  StructField("marital_status", StringType, true),
  StructField("occupation", StringType, true),
  StructField("relationship", StringType, true),
  StructField("race", StringType, true),
  StructField("sex", StringType, true),
  StructField("capital_gain", DoubleType, true),
  StructField("capital_loss", DoubleType, true),
  StructField("hours_per_week", DoubleType, true),
  StructField("native_country", StringType, true),
  StructField("income", StringType, true)
))

import sqlContext.implicits._
import org.apache.spark.sql.Row
val adultDataFrame = sqlContext.applySchema(censusValues.map(Row.fromSeq(_)), adultSchema)
adultDataFrame.show

/* Fix Missing Values */
adultDataFrame.groupBy(adultDataFrame("workclass")).count.foreach(println)
adultDataFrame.groupBy(adultDataFrame("occupation")).count.foreach(println)
adultDataFrame.groupBy(adultDataFrame("native_country")).count.foreach(println)

val adultDataFrameReplaced = adultDataFrame.na.replace(Array("workclass", "occupation"), Map("?" -> "Private"))
val adultDataFrameNoNa = adultDataFrameReplaced.na.replace(Array("native_country"), Map("?" -> "United-States"))

/* Converting strings to numeric values */
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame
def indexStringColumns(df:DataFrame, cols:Array[String]) : DataFrame = {
  var newdf = df
  for (c <- cols) {
    val si = new StringIndexer().setInputCol(c).setOutputCol(c + "-num")
    val sm:StringIndexerModel = si.fit(newdf)
    newdf = sm.transform(newdf).drop(c)
    newdf = newdf.withColumnRenamed(c + "-num", c)
  }
  newdf
}

val adultDataFrameNumeric = indexStringColumns(adultDataFrameNoNa, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"))

import org.apache.spark.ml.feature.OneHotEncoder
def oneHotEncodeColumns(df:DataFrame, cols:Array[String]) : DataFrame = {
  var newdf = df;
  for (c <- cols) {
    val onehotenc = new OneHotEncoder().setInputCol(c)
    onehotenc.setOutputCol(c + "-onehot").setDropLast(false)
    newdf = onehotenc.transform(newdf).drop(c)
    newdf = newdf.withColumnRenamed(c + "-onehot", c)
  }
  newdf
}

val adultDataFrameHot =oneHotEncodeColumns(adultDataFrameNumeric, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"))

import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(adultDataFrameHot.columns.diff(Array("income")))
val lpoints = va.transform(adultDataFrameHot).select("features", "income").withColumnRenamed("income", "label")

/*
  Fitting the model
*/

// Splitting the data into training and testing data setInputCols
val splits = lpoints.randomSplit(Array(0.8, 0.2))
val adultTrain = splits(0).cache
val adultTest = splits(1).cache


// Creating the model by fittin the algorithm
// option 1
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
var lr = new LogisticRegression
lr.setRegParam(0.01).setMaxIter(1000).setFitIntercept(true)
val lrModel = lr.fit(adultTrain)

lrModel.weights
lrModel.intercept

// option 2
import org.apache.spark.ml.param.ParamMap
val lrModel = lr.fit(adultTrain, ParamMap(lr.regParam -> 0.01, lr.maxIter -> 500, lr.fitIntercept -> true))

lrModel.weights
lrModel.intercept

/*
  Evaluating model's performance
*/
val testPredicts = lrModel.transform(adultTest)

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val binClassificationEvaluator = new BinaryClassificationEvaluator()
binClassificationEvaluator.evaluate(testPredicts)

// returns the area under receiver operating characteristic curve
binClassificationEvaluator.getMetricName

// changing the metric to obtain the area under precision-recall curve
binClassificationEvaluator.setMetricName("areaUnderPR")
binClassificationEvaluator.evaluate(testPredicts)

// Manually compute the PR curve
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
def computePRCurve(train:DataFrame, test:DataFrame, lrmodel:LogisticRegressionModel) = {
  for (threshold <- 0 to 10) {
    var thr = threshold / 10.0;
    if (threshold == 10) {
      thr -= 0.001
    }
    lrmodel.setThreshold(thr)
    val testpredicts = lrmodel.transform(test)
    val testpredRdd = testpredicts.map(row => (row.getDouble(4), row.getDouble(1)))
    val bcm = new BinaryClassificationMetrics(testpredRdd)
    val pr = bcm.pr.collect()(1)
    println("%.1f: R=%f, P=%f".format(thr, pr._1, pr._2))
  }
}

computePRCurve(adultTrain, adultTest, lrModel)

// Manually compute the Receiver Operating Characteristic curve
def computeROCCurve(train:DataFrame, test:DataFrame, lrmodel:LogisticRegressionModel) = {
  for (threshold <- 0 to 10) {
    var thr = threshold / 10.0
    if (threshold == 10) {
      thr -= 0.001
    }
    lrmodel.setThreshold(thr)
    val testpredicts = lrmodel.transform(test)
    val testpredRdd = testpredicts.map(row => (row.getDouble(4), row.getDouble(1)))
    val bcm = new BinaryClassificationMetrics(testpredRdd)
    val pr = bcm.roc.collect()(1)
    println("%.1f: FPR:%f, TPR=%f".format(thr, pr._1, pr._2))
  }
}

computeROCCurve(adultTrain, adultTest, lrModel)

/*
  Performing k-fold cross validation
*/
import org.apache.spark.ml.tuning.CrossValidator
val cv = new CrossValidator().setEstimator(lr).setEvaluator(binClassificationEvaluator).setNumFolds(5)

// Create the combinaiton of parameters as an array of ParamMaps
import org.apache.spark.ml.tuning.ParamGridBuilder
val paramGrid = new ParamGridBuilder().addGrid(lr.maxIter, Array(1000)).addGrid(lr.regParam, Array(0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5)).build()

cv.setEstimatorParamMaps(paramGrid)

val cvmodel = cv.fit(adultTrain)

// obtain the weights of the best logistic regression model
cvmodel.bestModel.asInstanceOf[LogisticRegressionModel].weights

// obtain the selected regularization parameter
cvmodel.bestModel.parent.asInstanceOf[LogisticRegression].getRegParam

// Evaluating the performance of the best model
new BinaryClassificationEvaluator().evaluate(cvmodel.bestModel.transform(adultTest))
