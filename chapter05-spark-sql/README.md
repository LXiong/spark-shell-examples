# Spark in Action Examples: Chapter 5 &mdash; Sparkling queries with Spark SQL
> Scala examples for Spark shell

## 01-create-dataframe-from-rdd
Illustrates the three different available methods to convert an RDD to a DataFrame, as well as some of the available capabilities to explore the DataFrame schema:
+ using RDDs containing row data as tuples
+ using case classes
+ specifying a schema (standard)

In the example, we use a dataset `italianPosts.csv` containing information from a particular StackExchange forum.

## 02-dataframe-api-basics
Illustrates the basics of the DataFrame API:
+ Selecting data (*columns*) from a DataFrame
+ Filtering data &mdash; column containing, column expressions, limit number of responses
+ Renaming and adding new columns
+ Sorting data

## 03-sql-functions
Illustrates how to use SQL functions:
+ Scalar functions
+ Aggregate functions
+ Window functions
+ User-Defined functions

## 04-handling-missing-values
Illustrates how to handle *nulls* and *NaN* values found in a DataFrame. Specifically, `drop`, `fill` and `replace` are used.

## 05-create-rdd-from-df
Demonstrates how to convert an RDD to a DataFrame after having performed a map operation.

## 06-df-grouping
Illustrates how to group *DataFrame* data using `groupBy`, `rollup` and `cube`.

## 07-df-joining
Illustrates how to perform join operations on *DataFrames* using `join`.

## 08-df-sql-commands
Illustrates how to perform SQL operations on *DataFrames* using SQL.

## 09-df-saving-data
Illustrates how to save DataFrames to file, and how to configure the writer.

## e01-spark-sql
Illustrates how to mix in a SQL a data source which is a file and a database table hosted on Postgres.
Note that the shell must be started configuring the classpath with the path of the driver: `SPARK_CLASSPATH=~/Downloads/postgresql-9.4.1208.jar bin/spark-shell --master local[*]`

Note also that the example requires the Postgres database to be prepared with a couple of tables with loaded data:
```sql
CREATE TABLE promotion__c
(
  date_from__c date,
  date_thru__c date,
  createddate timestamp without time zone,
  sfid character varying(18),
  slogan__c character varying(1300),
  anchor_account__c character varying(18),
  _hc_err text,
  slogan_language_1__c character varying(255),
  _hc_lastop character varying(32),
  name character varying(80),
  anchor_account__r__pkey__c character varying(22),
  systemmodstamp timestamp without time zone,
  id serial NOT NULL,
  isdeleted boolean,
  CONSTRAINT promotion__c_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE promotion_product
(
  promotion_sfid character varying(18) NOT NULL,
  product_sfid character varying(18) NOT NULL,
  CONSTRAINT promotion_product_pkey PRIMARY KEY (promotion_sfid, product_sfid)
)
WITH (
  OIDS=FALSE
);
```

*Note:*
A sample set of data can be found on the files `data/e01-data.sql`.


The join query is:
```sql
SELECT a.slogan__c, c.productCategory, c.productDescription from promotions a, promotion_product b, products c
WHERE a.sfid = b.promotion_sfid
 AND  c.productid = b.product_sfid;
```
