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
  Remove all the rows from postsDF that contain null or NaN values
*/
val cleanPostsDF = postsDF.na.drop
postsDF.count
cleanPostsDF.count

/* using `any` to obtain the same result */
postsDF.na.drop("any").count

/* using `all` means remove rows that have nulls in all the columns */
postsDF.na.drop("all").count

/* Remove specific columns */
postsDF.na.drop(Array("acceptedAnswerId")).count

/* mapping nulls to certain values */
val postsWithNoNullViewCountDF = postsDF.na.fill(Map("viewCount" -> 0))

/*
  Replace certain values in specific columns
*/

/* change value in `id` and `acceptedAnswerId`: 1177 -> 3000 */
val postsCorrectedDF = postsDF.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))
