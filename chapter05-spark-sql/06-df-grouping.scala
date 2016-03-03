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
  Grouping using groupBy
*/

/*
  groupBy
  Find the number of posts per author, associated tags and post types
 */
postsDF.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc).show(10)

/*
   agg
   find the last activity date and the maximum post score per user
*/
postsDF.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)

// alternatively...
postsDF.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)

// chaining expressions
postsDF.groupBy('ownerUserId).agg(max('lastActivityDate), max('score).gt(5) as "score > 5").show(10)

/*
  Rollup
*/
val simplifiedDF = postsDF.where('ownerUserID >= 13 and 'ownerUserId <= 15)
simplifiedDF.show

/* groupBy vs. rollup
   count the posts by owner, tags and post type using groupBy
*/
simplifiedDF.groupBy('ownerUserId, 'tags, 'postTypeId).count.show

/* rollup returns the same results as groupBy but adds
   + subtotals per owner,
   + per owner and tags and
   + the grand total
*/
simplifiedDF.rollup('ownerUserId, 'tags, 'postTypeId).count.show

/* cube function returns all of these results but also adds other possible subtotals
   (per post type, per tags, per post type and tag, per post type and user)
*/
simplifiedDF.cube('ownerUserId, 'tags, 'postTypeId).count.show
