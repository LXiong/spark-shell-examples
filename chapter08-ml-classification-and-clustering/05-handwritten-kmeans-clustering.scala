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

import org.apache.spark.mllib.linalg.Vector
val penflrdd = penlpoints.map(row => (row(0).asInstanceOf[Vector], row(1).asInstanceOf[Double]))
val penrdd = penflrdd.map(x => x._1).cache

import org.apache.spark.mllib.clustering.KMeans
val kmmodel = KMeans.train(penrdd, 10, 5000, 20)

kmmodel.computeCost(penrdd)
math.sqrt(kmmodel.computeCost(penrdd)/penrdd.count())

val kmpredicts = penflrdd.map(feat_lbl => (kmmodel.predict(feat_lbl._1).toDouble, feat_lbl._2))

//rdd contains tuples (prediction, label)
import org.apache.spark.rdd.RDD
def printContingency(rdd:RDD[(Double, Double)], labels:Seq[Int])
{
    val numl = labels.size
    val tablew = 6*numl + 10
    var divider = "".padTo(10, '-')
    for(l <- labels)
        divider += "+-----"

    var sum:Long = 0
    print("orig.class")
    for(l <- labels)
        print("|Pred"+l)
    println
    println(divider)
    val labelMap = scala.collection.mutable.Map[Double, (Double, Long)]()
    for(l <- labels)
    {
        //filtering by predicted labels
        val predCounts = rdd.filter(p => p._2 == l).countByKey().toList
        //get the cluster with most elements
        val topLabelCount = predCounts.sortBy{-_._2}.apply(0)
        //if there are two (or more) clusters for the same label
        if(labelMap.contains(topLabelCount._1))
        {
            //and the other cluster has fewer elements, replace it
            if(labelMap(topLabelCount._1)._2 < topLabelCount._2)
            {
                sum -= labelMap(l)._2
                labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
                sum += topLabelCount._2
            }
            //else leave the previous cluster in
        }
        else
        {
            labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
            sum += topLabelCount._2
        }
        val predictions = predCounts.sortBy{_._1}.iterator
        var predcount = predictions.next()
        print("%6d".format(l)+"    ")
        for(predl <- labels)
        {
            if(predcount._1 == predl)
            {
                print("|%5d".format(predcount._2))
                if(predictions.hasNext)
                    predcount = predictions.next()
            }
            else
                print("|    0")
        }
        println
        println(divider)
    }
    println("Purity: "+sum.toDouble/rdd.count())
    println("Predicted->original label map: "+labelMap.mapValues(x => x._1))
}
printContingency(kmpredicts, 0 to 9)
