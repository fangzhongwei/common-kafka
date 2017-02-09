package com.jxjxgo.common.kafka.template

import java.util
import java.util.Properties
import javax.inject.{Inject, Named}

import com.jxjxgo.common.mq.service.ConsumerService
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
  * Created by fangzhongwei on 2017/2/9.
  */
trait ConsumerTemplate {
  def init: Future[Unit]
}

class ConsumerTemplateImpl @Inject()(@Named("kafka.bootstrap.servers") servers: String,
                                     @Named("kafka.group.id") groupId: String,
                                     @Named("kafka.topics") topics: String,
                                     consumerService: ConsumerService) extends ConsumerTemplate {
  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)

  override def init: Future[Unit] = {
    val promise: Promise[Unit] = Promise[Unit]()
    Future {
      val props: Properties = buildProperties
      val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
      consumer.subscribe(util.Arrays.asList(topics.split(","): _*))
      val minBatchSize: Int = 1
      val buffer: ListBuffer[Array[Byte]] = ListBuffer[Array[Byte]]()
      while (true) {
        val records: ConsumerRecords[String, Array[Byte]] = consumer.poll(100)
        val iterator: util.Iterator[ConsumerRecord[String, Array[Byte]]] = records.iterator()
        while (iterator.hasNext) {
          buffer += iterator.next().value()
        }
        if (buffer.size >= minBatchSize) {
          try {
            logger.info(s"receive message : $buffer")
            consumerService.consume(buffer)
            consumer.commitSync
          } catch {
            case ex: Exception => logger.error("consumer", ex)
          }
          buffer.clear
        }
      }
      promise.success()
    }
  }

  def buildProperties: Properties = {
    val props: Properties = new Properties
    props.put("bootstrap.servers", servers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props
  }
}
