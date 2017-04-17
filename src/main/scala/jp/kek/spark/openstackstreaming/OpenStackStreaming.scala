package jp.kek.spark.openstackstreaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka._

object OpenStackStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = new SparkContext(new SparkConf().setAppName("OpenStack Streaming"))
    val ssc = new StreamingContext(sc, Milliseconds(5000))
    ssc.checkpoint("/tmp/checkpoint")
    val kafkaStream = KafkaUtils.createStream(ssc,
                                              args(0),
                                              "default",
                                              Map(args(1) -> 1))
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
