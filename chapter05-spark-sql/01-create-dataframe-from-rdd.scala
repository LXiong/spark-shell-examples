val csvFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter05-spark-sql/data/italianPosts.csv"

val itPostsLinesRDD = sc.textFile(csvFilename)
val itPostsFieldsRDD = itPostsLinesRDD.map(line => line.split("~"))
itPostsFieldsRDD.count // -> 1261

/*
  Method 1:
  Create a DataFrame from an RDD of tuples
*/

val itPostsRDD = itPostsFieldsRDD.map(postFields => (postFields(0), postFields(1), postFields(2), postFields(3), postFields(4), postFields(5), postFields(6), postFields(7), postFields(8), postFields(9), postFields(10), postFields(11), postFields(12)))
val itPostsDFrame = itPostsRDD.toDF()

/* unnamed columns */
itPostsDFrame.show

/* named columns */
val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
itPostsDF.show

itPostsDF.printSchema // -> all columns are nullable strings

/*
  Method 2:
  Create a DataFrame from an RDD using a case class
*/
import java.sql.Timestamp
case class Post(
  commentCount:Option[Int],
  lastActivityDate:Option[Timestamp],
  ownerUserId:Option[Long],
  body:String,
  score:Option[Int],
  creationDate:Option[Timestamp],
  viewCount:Option[Int],
  title:String,
  tags:String,
  answerCount:Option[Int],
  acceptedAnswerId:Option[Long],
  postTypeId:Option[Long],
  id:Long
)

object StringImplicits {
  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception.catching
    def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
    def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
    def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
  }
}

import StringImplicits._
def stringToPost(row:String): Post = {
  val r = row.split("~")
  Post(r(0).toIntSafe,
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

val itPostsDFCase = itPostsLinesRDD.map(row => stringToPost(row)).toDF

itPostsDFCase.show(5)

itPostsDFCase.printSchema

/*
  Method 3:
  Converting RDDs to DataFrames by specifying a schema
*/

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
val itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema)

itPostsDFStruct.show(5)

itPostsDFStruct.printSchema

/*
  Obtaining schema information from a DataFrame
*/

/* print the schema */
itPostsDFStruct.printSchema

/* accessing the schema associated to the DataFrame */
itPostsDFStruct.schema

/* obtaining the columns */
itPostsDFStruct.columns

/* obtaining the datatypes tuples [(column-name, column-type)] */
itPostsDFStruct.dtypes
