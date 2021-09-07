package io.github.devlibx.spark.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DataExporterFlatten extends App {

  // Name of Hudi table
  val tableName = "users_data"
  val tableLocation = "file:///tmp/table35/"

  // Topics to read from
  val topics = Array("test10")

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
    val dataNew = jsonData("json_data_string").str
    (dataNew)
  }).foreachRDD(rdd => {

    // Make a spark context from
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()

    // Make a frame to store
    val cachedRdd = rdd.cache()
    val finalRdd = spark.read.json(cachedRdd)
    finalRdd.explodeColumns
      .write
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "1") // How many spark jobs will run to insert (you should put more than 1)
      .option("hoodie.upsert.shuffle.parallelism", "1") // How many spark jobs will run to upsert (you should put more than 1)
      .option("hoodie.delete.shuffle.parallelism", "1") // How many spark jobs will run to delete (you should put more than 1)
      .option("hoodie.bulkinsert.shuffle.parallelism", "1") // How many spark jobs will run to bulk insert (you should put more than 1)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "user_id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition_id")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .mode(SaveMode.Append)
      .save(tableLocation)
  });

  ssc.start()
  ssc.awaitTermination()


  // Flatten the Json Data

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  import scala.annotation.tailrec

  implicit class DFHelpers(df: DataFrame) {
    def columns: Array[Column] = {
      val dfColumns = df.columns.map(_.toLowerCase)
      df.schema.fields.flatMap {
        case column if column.dataType.isInstanceOf[StructType] => {
          column.dataType.asInstanceOf[StructType].fields.map { field =>
            val columnName = column.name
            val fieldName = field.name
            col(s"${columnName}.${fieldName}").as(s"${columnName}_${fieldName}")
          }.toList
        }
        case column => List(col(s"${column.name}"))
      }
    }

    def flatten: DataFrame = {
      val empty = !df.schema.exists(_.dataType.isInstanceOf[StructType])
      if (empty) {
        df
      } else {
        df.select(columns: _*).flatten
      }
    }

    def explodeColumns: DataFrame = {
      @tailrec
      def columns(cdf: DataFrame): DataFrame = cdf.schema.fields.filter(_.dataType.typeName == "array") match {
        case c if !c.isEmpty => columns(c.foldLeft(cdf)((dfa, field) => {
          dfa.withColumn(field.name, explode_outer(col(s"${field.name}"))).flatten
        }))
        case _ => cdf
      }

      columns(df.flatten)
    }
  }
}
