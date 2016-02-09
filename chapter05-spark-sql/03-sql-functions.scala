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
  Scalar and Aggregate built-in functions
*/

/* find the difference that has been active for the largest amount of time */
postsDF.filter('postTypeId === 1).withColumn("activePeriod", datediff('lastActivityDate, 'creationDate)).orderBy('activePeriod desc).head.getString(3).replace("&lt;", "<").replace("&gt;", ">")

/* find the average and maximum score of all questions and the total number of questions */
postsDF.select(avg('score), max('score), count('score)).show

/*
  Window functions
*/

/* display the maximum score of all users' questions and for each question, how much its score is below the maximum score for that user */
import org.apache.spark.sql.expressions.Window
postsDF.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser").withColumn("toMax", 'maxPerUser - 'score).show(10)

/* for each question, display id of its owner's next and previous questions by creation date */
postsDF.filter('postTypeId === 1).select('ownerUserId, 'id, 'creationDate, lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev", lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").orderBy('ownerUserId, 'id).show(10)

/*
  User Defined functions
*/

/* find how many tags a each question has */
val countTags = udf((tags:String) => "&lt;".r.findAllMatchIn(tags).length)

/* alternatively using udf.register */
val countTagsAlt = sqlContext.udf.register("countTags", (tags:String) => "&lt;".r.findAllMatchIn(tags).length)

postsDF.filter('postTypeId === 1).select('tags, countTags('tags) as "tagCnt").show(10, false)
postsDF.filter('postTypeId === 1).select('tags, countTagsAlt('tags) as "tagCnt").show(10, false)
