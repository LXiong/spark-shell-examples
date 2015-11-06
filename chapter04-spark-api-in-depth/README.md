Spark in Action: Shell programs
===============================

# Chapter 4: Spark API in depth

## 01-trans-by-cust
Given the yesterday's transactions file `ch04_data_transactions.txt` create a Pair RDD consisting of:
```
{customerID, transactionData}
```

The transactions file is a delimited file, with each line consisting a single transaction, and each transaction field delimited by `#` as in:
```
2015-03-30#6:55 AM#51#68#1#9506.21
```
with the fields being:
  + the date of the purchase: 2015-03-30
  + the time of the purchase: 6:55 AM
  + the CustomerID: 51
  + the ProductID: 68
  + the Quantity: 1
  + the total price of the purchase: 9506.21

## 02-distinct-buyers
Based on the previous program, find the number of distinct buyers.

## 03-num-purchases-per-client
Based on the previous program, find the number of purchases made by each client.
And the client who bought the greatest number of products.
