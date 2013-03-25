hbase-utils
===========

### A utility library for HBase

#### Intro
Right now this is just a personal project to help me learn some Scala as well as the differences between HBase and Accumulo. It is very much a work in progress.

#### What Can it Do?
Currently, the project contains a client library and HBase coprocessor for creating and querying a keyword based index.
The client library will let you query for things you've indexed based on a list of keywords (terms) and will return items matching all or any of the terms. The result is a compressed bitmap where each bit represents a document.

##### Client
The client library provides an API for adding items to the index and also querying it.

##### Coprocessor
A coprocessor that handles the queries on the region servers.

##### Limitations
Limited to storing as many items as fit in a Java long (2^63-1) so, for most applications it should be plenty.

##### How does it work?
First, each item is assigned a unique integer identifier. This means you can use whatever you want as your original key.
Ids are requested from Zookeeper to ensure their uniqueness.
Next, rows for each term are inserted mapping them to the id as well as a mapping between the original item id and integer id.

Upon querying, the coprocessor creates a compressed bitmap for each specified term and sets the bit corresponding to the
integer id of each item in the bitmap. For a query where ALL terms must be present for an item, these bitmaps are then
ANDed together to create a final bitmap. This bitmap now has bits set for each document that matches ALL terms.

This bitmap can then be expanded to the original item ids and returned to the querying client.

#### TODO
Final scan to map the bitmap based id to the original item id.
