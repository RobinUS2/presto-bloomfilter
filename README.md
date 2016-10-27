# presto-bloomfilter [![Build status](https://api.travis-ci.org/RobinUS2/presto-bloomfilter.svg)](https://travis-ci.org/RobinUS2/presto-bloomfilter)
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
3.  Once this is complete there will basically be a single bloom filter instance which can help us tell whether a customer ID is part of the set, or not. Due to the characteristics of a bloom filter this doesn't require to send the full list of members of the set, reducing memory and network overhead.
4.  We then go in the regular select to get the order data. 
5.  The where statement uses `bloom_filter_contains` to find out whether an element is probably in a set. False positives are possible.

Functions
-------------
There are tho types of functions, aggregation and scalar. 

### Aggregation
`bloom_filter(<element:VARCHAR>)` -> BloomFilter

Will create a Bloom Filter with default settings. Expected insertions are 10 000 000 items with an accepted false positive percentage of 1% (0.01).

`bloom_filter(<element:VARCHAR>, <expected_insertions:INT>)` -> BloomFilter

Will create a Bloom Filter with expected inesrtions hint (amount of elements you expect to put in the Bloom Filter) default false positive settings.

`bloom_filter(<element:VARCHAR>, <expected_insertions:INT>, <false_positive_percentage:DOUBLE)` -> BloomFilter

Will create a Bloom Filter with custom settings. Percentage should be in the range [0-1].

### Scalar
`bloom_filter_contains(<BloomFilter>, <element>)` -> boolean

Returns ``TRUE`` if the item is probably in the set and returns ``FALSE`` if it is definitely not in there.

### Serialization
`to_string(<BloomFilter>)` -> VarChar

Will serialized (~ convert) a Bloom Filter with all it's settings to a string.

`bloom_filter_from_string(<element:VARCHAR>)` -> BloomFilter

This will load a previously serialized string back into a Bloom Filter object.

Persistence
-------------
It might be the case that you can actually pre-compute your bloom filters and re-use them for a certain period to run your queries on. In order to support this we have included a very light weight, high performance http key-value store. 

This simply allows you to persist the state of a bloom filter computed in presto to the service and load it back in another query. This reduces the population of the bloom filter from the full query time (seconds to minutes range) to neglible (very low milliseconds range).

### Persistence functions
`bloom_filter_load('<url:VARCHAR>')` -> BloomFilter

This will load a bloom filter from the persistence service with a given key.

`bloom_filter_persist(<BloomFilter>, '<url:VARCHAR>')` -> boolean

This will persist a bloom filter to the persistence service with a given key.

### How to run the service
Simply go into the folder `persist-service` and run the `./build.sh` script. This should produce a binary with the name `persist-service`. Then create an configuration file in `/etc/prestobloomfilterpersist.json` with the contents `{}`. Once you then start the process it will start listening on port `8081`. 

### Cassandra as backend
Keyspace
```
CREATE KEYSPACE mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;
```

Table
```
CREATE TABLE mytable (key varchar PRIMARY KEY, value varchar);
```

Config
```
{
    "Backend": "cassandra",
    "Cassandra": {
        "ProtoVersion": 3,
        "Keyspace": "mykeyspace",
        "Table": "mytable",
        "Hosts": ["127.0.0.1"],
        "Consistency": "QUORUM"
    }
}
```

### How to construct the url
Let's say you are running the persist service on a host with as hostname `my-persist-service.internal` on port `8081`. You can then have a URL like this: `http://my-persist-service.internal:8081/bloomfilter/my-first-bf` where `my-first-bf` is the actual key under which it's stored/loaded.

### Full example
Step 1 - Loading the data and saving it into a persisted bloom filter
```
WITH input AS (SELECT 'one' AS val UNION SELECT 'two' AS val UNION SELECT 'three' AS val)
SELECT bloom_filter_persist(bloom_filter(input.val), 'http://my-persist-service.internal:8081/bloomfilter/my-first-bf') FROM input;
```
yields
```
 _col0 
-------
 true  
(1 row)
```

Step 2 - Using the previously persisted bloom filter on your data
```
WITH input AS (SELECT 'one' AS val UNION SELECT 'two' AS val UNION SELECT 'three' AS val UNION SELECT 'four' AS val UNION SELECT 'five' AS val), 
bf AS (SELECT bloom_filter_load('http://my-persist-service.internal:8081/bloomfilter/my-first-bf') AS bf) 
SELECT val, bloom_filter_contains(bf.bf, input.val) FROM input, bf;
```
yields
```
  val  | _col1 
-------+-------
 three | true  
 two   | true  
 one   | true  
 four  | false 
 five  | false 
(5 rows)
```

Bloom Filters
-------------
This project uses the [Bloom Filter](https://en.wikipedia.org/wiki/Bloom_filter) probabilistic data structure to keep track whether an element is part of a set.

Building
-------------
As this is a Presto plugin it relies on presto-spi. This means you will have to build Presto first and this project expects to find Presto as a parent project. Basically set it up like this:

/code/  
/code/presto  
/code/presto-bloomfilter  

Then run `mvn clean install` in /code/presto. Once that has finished run `mvn clean install` in /code/presto-bloomfilter.

This will provide a .zip file in the target/ folder which you can unpack and then copy into the presto server plugin/ folder.
