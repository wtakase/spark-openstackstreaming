package jp.kek.spark.openstackstreaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.elasticsearch.spark.rdd.EsSpark
import kafka.serializer.StringDecoder
import scala.util.parsing.json.JSON

object OpenStackStreaming {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val brokerList = args(0)
    val topicId = args(1)
    val esNodes = args(2)
    val timestamp: Long = System.currentTimeMillis / 1000
    val checkpoint = "/tmp/checkpoint" + "/" + topicId + "/" + timestamp.toString

    val ssc = StreamingContext.getOrCreate(checkpoint,
                () => createStreamingContext(brokerList, topicId, esNodes, checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(
          brokerList: String, topicId: String, esNodes: String,
          checkpoint: String): StreamingContext = {
    val conf = new SparkConf().setAppName("OpenStack Logs Streaming").set("es.nodes", esNodes)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(5000))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                        ssc, kafkaParams, Set(topicId))

    run(kafkaStream)
    ssc.checkpoint(checkpoint)
    ssc
  }

  def run(stream: InputDStream[(String, String)], windowLength: Int = 30, slideInterval: Int = 5) {
    stream.foreachRDD(rdd => rdd.collect().foreach(println))
    stream.map(_._2).map(JSON.parseFull(_)).foreachRDD(rdd => EsSpark.saveToEs(rdd, "openstack/logs"))
  }
}
