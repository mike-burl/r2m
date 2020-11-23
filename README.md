# MarkLogic PreJoiner
Loading relational data into MarkLogic using the Document-per-row approach is an anti-pattern due to the high amount of both CPU and disk overhead.  Joining these documents before loading can dramatically increase both ingestion and harmonization performance.  The r2m tool is for joining many rows from individual relational tables into larger JSON documents and then loading them into a MarkLogic cluster.  It was designed to be fast, memory efficient, and scalable.

This process occurs in three steps:
1. An entity residing in a relational source system is identified and defined in a config file.  SQL is used to define how the tool will pull both the root and child tables.
2. The Java tool will connect to the source database and query the root table identified in the properties file to build an entity, extract keys, use that key to find any child rows in associated source tables, then build a full JSON document containing all the information.
3. The JSON document is then sent to [a bulk Data Service loader](https://docs.marklogic.com/guide/java/DataServices "Java Application Developer's Guide - Data Services") for batching and distributed loading into the MarkLogic cluster.

## Configuring the Tool
The tool is driven by three config files:
1. **marklogicConfiguration.json** : This contains all information used for connecting to your MarkLogic cluster
   * **hosts** : Array of addresses for all the E-nodes in the cluster
   * **port** : The port number associated with the MarkLogic database's application server
   * **numThreadsPerHost** : The number of concurrent threads that will load batches of documents to each node in the cluster
   * **batchSize** : The maxmimum number of documents that will be sent in each batch
   * **username** : The MarkLogic username that will be used to load the documents
   * **password** : The password to the above username
   * **authContext** : The authorization context used by the MarkLogic application server
2. **entityInsert.json** : This contains the information that will be applied to the document as it is loaded into the cluster
   * **entityName** : The name of the root entity represented by the full JSON document
   * **uriPrefix** : The prefix that will be appended to all generated document URIs
   * **uriSuffix** : The suffix that will be appended to all generated document URIs
   * **keys** : An array of column names that will be used to generate the document's unique key
   * **keyDelimiter** : The character or sequence of characters that will be inserted between each key in the URI
   * **collections** : An array of collections that will be tagged onto each document
3. **entityJoiner.json** : This contains the information that will be used to gather the entity rows from the source system and bind them together
   * **query** : The SQL that will be used to gather the table's rows
   * **propertyName** : The name that will be applied to this table's JSON property as it's constructed into a document
   * **primaryKeyColumnNames** : An array of columns used to identify the parent primary keys
   * **foreignKeyColumnNames** : An array of columns used to identify the child primary keys
   * **foreignKeyQuotedValues** : An array of columns that need to be encapsulated in quotes in order to be queried properly
   * **childQueries** : An array of objects defining the child rows to this table.  This is recursive in nature with no theoretical limit on depth

Gradle is used for deploying the MarkLogic-side of the data service and can also be used to build and run the tool.  The settings for this are located in the **gradle.properties** file located in the root directory.

## Using the Tool
1. **Deploy the data service** : Use gradle to deploy the data service to the MarkLogic cluster with the command './gradlew mlLoadModules -i'.  Verify that a module with the name bulkLoader.xqy was inserted into the modules database.
2. **Build the r2m app** : Use gradle to build the prejoiner Java app with the command './gradlew build'.  This will automatically resolve all dependencies the tool needs to run.
3. **Run the application** : Use gradle to launch the r2m Java app with the command './gradlew r2m:run --args='-db jdbc:h2:./data/h2/client -jc ./config/entityConfig.json -mi ./config/entityInsertConfig.json -mc ./marklogicConfiguration.json'.  The args are as follows:
   * **-db** : The first argument specifies the type of JDBC connector to use, depending on the type of database being queries.
   * **-jc** : The path to the join configuration json
   * **-mi** : The path to the marklogic insert configuration json
   * **-mc** : The path to the marklogic connection configuration json
4. **That's it!** : Monitor the MarkLogic cluster, ingestion server, and source system to ensure resources are being utilized effectively.  Modify the thread and batch parameters as you see fit.

## Dependencies
* [MarkLogic Server](https://developer.marklogic.com/products/marklogic-server/10.0 "MarkLogic 10 - MarkLogic Developer Community")
* [Gradle](https://gradle.org/ "Gradle Build Tool")
* [Java Virtual Machine 1.8](https://www.java.com/en/ "Java | Oracle")