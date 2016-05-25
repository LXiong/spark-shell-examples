val csvProductsFilename = "/home/ubuntu/Development/git-repos/spark-in-action-repo/spark-shell-examples-parent/chapter05-spark-sql/data/e01-products.csv"

val productsLinesRDD = sc.textFile(csvProductsFilename)
productsLinesRDD.count // -> 340


/*
  Using the standard method to convert an RDD to a DataFrame (specifying a schema)
*/
import org.apache.spark.sql.types._
val productsSchema = StructType(Seq(
  StructField("productId", StringType, true),
  StructField("productDescription", StringType, true),
  StructField("productCategory", StringType, true)
))

import StringImplicits._
import org.apache.spark.sql.Row
def stringToRow(row:String): Row = {
  val r = row.split(";")
  Row(r(0),
      r(1),
      r(2))
}

val rowRDD = productsLinesRDD.map(row => stringToRow(row))
val productsDF = sqlContext.createDataFrame(rowRDD, productsSchema)

productsDF.show(5)
productsDF.printSchema

/* non-persisting table registration on Spark's in-memory catalog */
productsDF.registerTempTable("products_temp")

/* This is freaking amazing */
val resultDF = sql("SELECT * FROM products_temp")
resultDF.show // sanity-check

/* more contrived example */
val resultDF = sql("SELECT * FROM products_temp WHERE productCategory = 'Beverages'")
resultDF.show

resultDF.registerTempTable("beverages_temp")

sql("SELECT COUNT(*) FROM beverages_temp WHERE productDescription LIKE '%Cola%'").show

val colaBeverages = sql("SELECT * FROM beverages_temp WHERE productDescription LIKE '%Cola%'")
colaBeverages.show

import org.apache.spark.sql.SaveMode
productsDF.write.mode(SaveMode.Overwrite).saveAsTable("products")

val promotionsJdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:postgresql://localhost/cas_local_dev", "dbtable" -> "promotion__c", "password" -> "postgres", "user" -> "postgres")).load()
promotionsJdbcDF.registerTempTable("promotions_temp")
promotionsJdbcDF.write.mode(SaveMode.Overwrite).saveAsTable("promotions")

val promotionProductsJdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:postgresql://localhost/cas_local_dev", "dbtable" -> "promotion_product", "password" -> "postgres", "user" -> "postgres")).load()
promotionProductsJdbcDF.registerTempTable("promotion_product_temp")
promotionProductsJdbcDF.write.mode(SaveMode.Overwrite).saveAsTable("promotion_product")

val resultDF = sql("SELECT a.slogan__c, c.productCategory, c.productDescription from promotions a, promotion_product b, products c WHERE a.sfid = b.promotion_sfid AND  c.productid = b.product_sfid")
resultDF.show()
