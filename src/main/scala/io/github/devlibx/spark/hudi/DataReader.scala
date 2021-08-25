package io.github.devlibx.spark.hudi

import org.apache.spark.sql.SparkSession

object DataReader extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val tableName = "users_data"
  val basePath = "file:///tmp/table9/"
  val tripsSnapshotDF = spark.read.format("hudi").load(basePath + "/*/*")
  tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
  spark.sql("select * from  hudi_trips_snapshot where user_id = 1").show(1000000)
}
