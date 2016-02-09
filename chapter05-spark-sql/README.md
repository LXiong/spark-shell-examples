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
