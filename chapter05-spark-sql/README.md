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
