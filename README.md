# ALSME - Another LSM Engine

[![Build Status](https://app.travis-ci.com/sprosin/ALSME.svg?branch=master)](https://app.travis-ci.com/sprosin/ALSME)

The idea of this project is to create simple yet powerful log-structured merge-tree (LSM) key-value storage.

The engine consists of three major components:
* memtable - tree-based structure to work with recently added values
* write-ahead log - to guarantee no data loss in case of any failure
* SSTable provider - to work with Sorting String Table data files

Apache Avro is used to serialize and deserialize the data.

This is an alpha version with a lack of key features that haven't been implemented yet:
* deletion of values - there is no functionality to delete the value once it was added to the storage; it could only be updated to the new value
* compaction - current version doesn't do compaction at all, which basically means that the engine is going to store all the values of the same key, which go through memtable to file storage
* multi-threading support - current version assumes that just one thread will be working with the engine; in case you need to use it from different threads, you have to implement synchronization in the app
* rate-limiting - the idea of rate limiting is to ensure the compaction mechanism is able to work in the background considering workload; since compaction is not implemented yet, rate-limiting makes no sense
