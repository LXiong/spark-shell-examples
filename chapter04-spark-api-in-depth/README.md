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

## 12-trans-by-prod
Illustrates how to build a pair RDD whose tuple key is the productID and the value is the transaction. This is built both from scratch reading the file and also by re-mapping the `trxByCust` pair RDD.

## 13-totals-by-prod
Illustrate how to find the total sold by productID, that is, a pairRDD whose tuple key is the productID and the value is the sum of all the transactions for that product.

## 14-products
Given the products data file `ch04_data_products.txt`, build a pair RDD consisting of:
```
{productID, product-data}
```

The product file is a delimited file, with each line consisting a single product, and each product field delimited by `#` as in:
```
17#LEGO Galaxy Squad#5593.16#4
```

with the fields being:
  + the productID: 17
  + the name of the product: LEGO Galaxy Squad
  + the unitary price for the product: 5593.16
  + the ??: 4

## 15-totals-and-products-join
Illustrate how to obtain the pair RDD resulting of joining the totals by product and the products. As a result, we will end up with a pairRDD (productID, (total, product-attributes))

## 16-products-not-sold-yesterday
Illustrates how to obtain a pair RDD with the products that were not sold yesterday using a `join` operations, `subtractByKey` and `cogroup`.

## 17-intersection
Illustrates how to use the `intersection` transformation.

## 18-cartesian
Illustrates how to use `cartesian` to perform the cartesian product of two RDDs. There are two examples on the file, one that performs the cartesian product of an RDD of characters and another of numbers; and another that performs the cartesian product of two numeric RDDs and then filters the pairs that are divisible.

## 19-zip
Illustrates how to use the `zip` transformation.

## 20-zip-partitions
Illustrates how to use the `zipPartitions` transformation.

## 21-totals-by-prod-sortby
