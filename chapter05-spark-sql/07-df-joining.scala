val csvPostsFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter05-spark-sql/data/italianPosts.csv"

val itPostsLinesRDD = sc.textFile(csvPostsFilename)
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

val csvVotesFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter05-spark-sql/data/italianVotes.csv";
val itVotesLinesRDD = sc.textFile(csvVotesFilename)
itVotesLinesRDD.count // -> 6348

val votesFieldsRDD = itVotesLinesRDD.map(line => line.split("~"))

//import org.apache.spark.sql.Row
val votesRowsRDD = votesFieldsRDD.map(fields => Row(fields(0).toLong, fields(1).toLong, fields(2).toInt, Timestamp.valueOf(fields(3))))

val votesSchema = StructType(Seq(
  StructField("id", LongType, false),
  StructField("postId", LongType, false),
  StructField("voteTypeId", IntegerType, false),
  StructField("creationDate", TimestampType, false)
))

val votesDF = sqlContext.createDataFrame(votesRowsRDD, votesSchema)
votesDF.show(10)

/*
  Joining DataFrames
*/

/* inner join:
    For the keys that exist in only one of the DFs, the resulting DF will have no elements.
 */
val postsVotesDF = postsDF.join(votesDF, postsDF("id") === 'postId)
postsVotesDF.show(5)
postsVotesDF.count // -> 6182

/* outer join:
    All the elements from the both RDDs will be present in the resulting RDD.
*/
val postsVotesOuterDF = postsDF.join(votesDF, postsDF("id") === 'postID, "outer")
postsVotesOuterDF.count // -> 6503
