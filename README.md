### How to build

```scala
mvn clean install - P idea   
```

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