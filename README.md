### How to build

```scala
mvn clean install - P idea   
```

#### Exporter

DataExporter class is used to read data from kafka every 10 sec (can be changed). This data is then saved in a directory
as Hudi table

#### Reader

Simple spark reader to read the Hudi table.