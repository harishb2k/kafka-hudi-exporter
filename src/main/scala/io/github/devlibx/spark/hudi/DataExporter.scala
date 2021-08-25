package io.github.devlibx.spark.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DataExporter extends App {

  // Name of Hudi table
  val tableName = "users_data"
  val tableLocation = "file:///tmp/table9/"

  // Topics to read from
  val topics = Array("test4")

  // Name of the columns in your data
  // "ts" column is must (most of the time it is the timestamp)
  val colNames = Seq("key", "user_id", "partition_id", "data", "ts")

  val conf = new SparkConf()
    // Hudi needs spark.serializer as Kryo (Mandatory)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local[*]")
    .setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(10))

  // Basic kafka stream setup
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.map(record => {
    val jsonData = ujson.read(record.value())
    val userId = jsonData("user_id").str
    val partitionId = jsonData("partition_id").str
    val data = jsonData("data").str
    val timestamp = jsonData("ts").num
    (record.key(), userId, partitionId, data, timestamp)
  }).foreachRDD(rdd => {

    // Make a spark context from
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    // import spark.implicits._
    import spark.implicits._

    rdd.toDF().toDF(colNames: _*)
      .write
      .format("hudi")
      // .option("hoodie.parquet.max.file.size", 1024)        // Max size of parquet file
      .option("hoodie.insert.shuffle.parallelism", "1") // How many spark jobs will run to insert (you should put more than 1)
      .option("hoodie.upsert.shuffle.parallelism", "1") // How many spark jobs will run to upsert (you should put more than 1)
      .option("hoodie.delete.shuffle.parallelism", "1") // How many spark jobs will run to delete (you should put more than 1)
      .option("hoodie.bulkinsert.shuffle.parallelism", "1") // How many spark jobs will run to bulk insert (you should put more than 1)
      // .option("hoodie.datasource.hive_sync.enable", "false")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "user_id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition_id")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .mode(SaveMode.Append)
      .save(tableLocation)

    // RECORDKEY_FIELD_OPT_KEY - this is the key which is a PK for your data
    // PARTITIONPATH_FIELD_OPT_KEY - this is used to partition data e.g. citi, month, country etc

  });

  ssc.start()
  ssc.awaitTermination()
}
