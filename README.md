### How to build

```scala
mvn clean install - P idea   
```
You may have to change the POM to generate a FatJar. For now you can run it from IDE.

#### Exporter

DataExporter class is used to read data from kafka every 10 sec (can be changed). This data is then saved in a directory
as Hudi table

#### Reader

Simple spark reader to read the Hudi table.

#### Data to generate for this example to run
Push data to kafka with following data
```json
{
  "user_id":     "user_123",
  "partition_id": "some_city",
  "data":         "some data",
  "ts":           "current_timestamp_as_integer",
}
```
<br>

### Flatten Json and store
Suppose you have a changing Json data, and you want to flatten it and store in Hudi.
You can see a sample ```DataExporterFlatten``` for the same.
```scala
    val cachedRdd = rdd.cache()
    val finalRdd = spark.read.json(cachedRdd)
    finalRdd.explodeColumns
      .write
      .continue....

// This DataExporterFlatten class defined a converted to do the flatten process
```
