Search Engine Log Analyzer for FarsBase Semantic Search
==========

# Introduction
This repository containss FarsBase query log analyzer. It automatically fetches the entities from the FarsBase KG store, and process the query log.

The output contains frequent patterns for semantic queries that are KG-friendly, i.e., the KG contains the data for responding to those patterns.  


# Requirement
* JDK 1.8
* Maven

# How to use
```
    mvn clean package
    java -jar target/LogAnalyzer-1.2-SNAPSHOT.jar
```


## License
The source code of this repo is published under the [Apache License Version 2.0](https://github.com/AKSW/jena-sparql-api/blob/master/LICENSE).



