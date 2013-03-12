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

#### TODO
Final scan to map the bitmap based id to the original item.
