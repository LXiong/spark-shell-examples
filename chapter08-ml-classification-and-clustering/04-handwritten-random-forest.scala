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

// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index
import org.apache.spark.ml.feature.StringIndexer
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(penlpoints)

import org.apache.spark.ml.feature.VectorIndexer
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(10).fit(penlpoints)


/*
  Fitting the model
*/

// Splitting the data into training and testing data setInputCols
val pendtsets = penlpoints.randomSplit(Array(0.8, 0.2))
val pendtTrain = pendtsets(0).cache
val pendtTest = pendtsets(1).cache

pendtTrain.count
pendtTest.count

// Train a Random Forest Model
import org.apache.spark.ml.classification.RandomForestClassifier
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxDepth(20)

// Convert indexed labels back to original labels
import org.apache.spark.ml.feature.IndexToString
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Chain indexer and tree in a Pipeline
import org.apache.spark.ml.Pipeline
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

// Train the model
val model = pipeline.fit(pendtTrain)

// Cast to a DecisionTreeClassificationModel to obtain metrics and tree information
import org.apache.spark.ml.classification.RandomForestClassificationModel
val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
rfModel.trees

rfModel.toDebugString

// Construct the predictions
val predictions = model.transform(pendtTest)
predictions.select("predictedLabel", "label", "features").show(5)

// Evaluate
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision")
val precision = evaluator.evaluate(predictions)
precision

val penPreds = predictions.select("prediction", "indexedLabel").map(row => (row.getDouble(0), row.getDouble(1)))

// Construct the MulticlassMetrics object
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val penmm = new MulticlassMetrics(penPreds)

// Obtaining the confusion matrix
// (i,j) = number of elements from the i class that were classified as j class
penmm.confusionMatrix
