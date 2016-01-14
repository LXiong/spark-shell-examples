Spark in Action: Shell programs
===============================

# Chapter 8: Machine learning: classification and clustering

## 01-census-logistic-regression
Illustrates how to use a *Logistic Regression Model* on a dataset consisting in census data which includes several independent variables and a target value which is the annual income of the individual.
The code is profusely commented, so please review the inline comments for all the details.

## 02-handwritten-multiclass
Illustrates how to use a *Multiclass Logistic Regression* on a dataset consisting in information from writers using a pen to draw digits from 0 to 9 (therefore, we have 10 possible values or classes for the label).

## 03-handwritten-decision-tree
Illustrates how to construct a *Decision Tree* on a dataset consisting in information from writers using a pen to draw digits from 0 to 9.
Note that the book's original listing (as of today, MEAPv06) does not work on my current version of Spark, so i've had to modify it browsing Spark's documentation for ML Decision Trees on: https://spark.apache.org/docs/1.5.0/ml-decision-tree.html
