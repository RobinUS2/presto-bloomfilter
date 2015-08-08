# presto-bloomfilter
Bloomfilter support for Facebook Presto (prestodb.io) 

Use cases & Examples
-------------
This project is very helpful if you want to "join" massive data sets very quickly. A typical query (before using bloom filters) would look something like this:

```
   SELECT orders.country, count(distinct orders.id) AS purchases 
   FROM customers 
   JOIN orders ON (customers.id=orders.customer_id) 
   WHERE customers.vip='1'
   GROUP BY 1 ORDER BY 2 DESC
```

In order to execute this query it will have to combine all records in the customers table with all the records in the orders table. This means sending a lot of data back and forth.

Now see how we can speed this up:

```
   WITH vips AS (
    SELECT bloom_filter(customer_id) AS bf 
    FROM customers 
    WHERE vip='1'
   ) 
   SELECT orders.country, count(distinct orders.id) AS purchases 
   FROM orders, vips 
   WHERE bloom_filter_contains(vips.bf, orders.customer_id)
   GROUP BY 1 ORDER BY 2 DESC
```

Let's go through this step by step:

1.  The WITH-statement is basically a sub query which will go through the customers table. We only need the VIPs, and we want to have all the customer IDs in the bloom filter structure.
2.  We stream all the customer IDs through the `bloom_filter` aggregation function. 
3.  Once this is complete there will basically be a single bloom filter instance which can help us tell whether a customer ID is part of the set, or not.
4.  We then go in the regular select to get the order data. 
5.  The where statement uses `bloom_filter_contains` to find out whether an element is probably in a set. False positives are possible.

Functions
-------------
There are tho types of functions, aggregation and scalar. 

### Aggregation
`bloom_filter(<element>)` -> BloomFilter

Will create a Bloom Filter with default settings. Expected insertions are 10 000 000 items with an accepted false positive percentage of 1% (0.01).

### Scalar
`bloom_filter_contains(<BloomFilter>, <element>)` -> boolean

Returns ``TRUE`` if the item is probably in the set and returns ``FALSE`` if it is definitely not in there.

Bloom Filters
-------------
This project uses the [Bloom Filter](https://en.wikipedia.org/wiki/Bloom_filter) probabilistic data structure to keep track whether an element is part of a set.