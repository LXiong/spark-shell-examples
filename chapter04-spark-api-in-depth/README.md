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

## 04-find-customer-transactions
Based on the previous program, find the transactions for a particular customer id.

## 05-apply-discount-to-specific-products
Based on the previous program, apply a 5% discount to orders with 2 or more products with productID = 25 (Barbie Shopping Mall).

## 06-add-complimentary-products
Based on the previous program, add a complimentary product with productID = 70 to all customers who bought 5 or more ProductID = 81 products.

## 07-find-customer-who-spent-more-overall-reducekey
Based on the previous program, find the customerID of the customer who spent the most overall using `reduceByKey`.

## 08-find-customer-who-spent-more-overall-foldbykey
Based on the previous program, find the customerID of the customer who spent the most overall using `foldByKey`.

## 09-add-complementary-gifts
Based on the previous program, add:
  + Include a complementary productID 4 to the client who bought the greatest number of products
  + Include a complementary productID 63 to the client who spent the most

This exemplifies how to add entries to an existing RDD.

## 10-find-customer-products-map
Based on the same scenario, build a map or {custID, list of products purchased}.

## 11-glom
Illustrates the `glom` transformation.
