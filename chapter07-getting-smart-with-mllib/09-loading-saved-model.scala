/*
  Load a previously saved model
*/

var pathToModel = "file:///home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter07-getting-smart-with-mllib/data/housing-dataset/model"

import org.apache.spark.mllib.regression.LinearRegressionModel;
val model = LinearRegressionModel.load(sc, pathToModel)

println(model.weights.toArray.map(x => x * x).zipWithIndex.sortBy(_._1).mkString(", "))
