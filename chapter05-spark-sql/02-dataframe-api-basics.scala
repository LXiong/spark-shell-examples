val csvFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter05-spark-sql/data/italianPosts.csv"

val itPostsLinesRDD = sc.textFile(csvFilename)
itPostsLinesRDD.count // -> 1261


/*
  Using the standard method to convert an RDD to a DataFrame (specifying a schema)
*/
import java.sql.Timestamp
object StringImplicits {
  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception.catching
    def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
    def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
    def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
  }
}

import org.apache.spark.sql.types._
val postSchema = StructType(Seq(
  StructField("commentCount", IntegerType, true),
  StructField("lastActivityDate", TimestampType, true),
  StructField("ownerUserId", LongType, true),
  StructField("body", StringType, true),
  StructField("score", IntegerType, true),
  StructField("creationDate", TimestampType, true),
  StructField("viewCount", IntegerType, true),
  StructField("title", StringType, true),
  StructField("tags", StringType, true),
  StructField("answerCount", IntegerType, true),
  StructField("acceptedAnswerId", LongType, true),
  StructField("postTypeId", LongType, true),
  StructField("id", LongType, false)
))

import StringImplicits._
import org.apache.spark.sql.Row
def stringToRow(row:String): Row = {
  val r = row.split("~")
  Row(r(0).toIntSafe,
       r(1).toTimestampSafe,
       r(2).toLongSafe,
       r(3),
       r(4).toIntSafe,
       r(5).toTimestampSafe,
       r(6).toIntSafe,
       r(7),
       r(8),
       r(9).toIntSafe,
       r(10).toLongSafe,
       r(11).toLongSafe,
       r(12).toLong)
}

val rowRDD = itPostsLinesRDD.map(row => stringToRow(row))
val postsDF = sqlContext.createDataFrame(rowRDD, postSchema)

postsDF.show(5)
postsDF.printSchema

/*
  Selecting data
*/

/* selecting specifying column names */
val postsIdBodyDF = postsDF.select("id", "body")

/* selecting specifyin columns using col method */
val postsIdBodyDF = postsDF.select(postsDF.col("id"), postsDF.col("body"))

/* selecting specifying Columns */
val postsIdBodyDF = postsDF.select(Symbol("id"), Symbol("body"))

/* using implicit $ that converts Strings to ColumnNames */
val postsIdBodyDF = postsDF.select($"id", $"body")

/* selecting all existing columns but the one given */
val postsIdDF = postsIdBodyDF.drop("body")

/*
  Filtering data
*/

/* find the count of all posts containing a given word */
postsIdBodyDF.filter('body contains "Italiano").count

/* find all questions that do not have an accepted answer */
val noAnswerDF = postsDF.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))

/* find first ten questions */
val firstTenQsDF = postsDF.filter('postTypeId === 1).limit(10)

/*
  Renaming columns
*/
val firstTenQsRenamedDF = firstTenQsDF.withColumnRenamed("ownerUserId", "owner")
firstTenQsRenamedDF.printSchema // -> no longer ownerUserId and just owner

/*
  Adding new columns
*/

/* construct a data frame from postsDF that includes a new column ratio = viewCount/score */
val enhancedPostsDF = postsDF.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score)
enhancedPostsDF.where('ratio < 35).show

/* 
  Sorting data
*/

/* find the 10 most recently modified questions */
val tenMostRecentlyModQsDF = postsDF.filter('postTypeId === 1).orderBy('lastActivityDate.desc).limit(10)
tenMostRecentlyModQsDF.show
