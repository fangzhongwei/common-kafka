package com.jxjxgo.common.kafka.template

import java.nio.charset.StandardCharsets
import java.util.Properties
import javax.inject.{Inject, Named}

import org.apache.kafka.clients.producer.{Callback, _}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by fangzhongwei on 2017/2/8.
  */
trait ProducerTemplate {
  def init: Unit

  def send(topic: String, data: Array[Byte]): Unit

  def send(topic: String, data: Array[Byte], callback: Callback): Unit

  def close: Unit
}

class ProducerTemplateImpl @Inject()(@Named("kafka.bootstrap.servers") servers: String) extends ProducerTemplate {
  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)
  private[this] var connected: Boolean = false

  private[this] var producer: Producer[String, Array[Byte]] = _

  override def init(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        initProducer
      }
    }).start()
  }

  init

  def initProducer: Unit = {
    try {
      val props: Properties = parseProducerConfig
      producer = new KafkaProducer[String, Array[Byte]](props)
      producer.send(new ProducerRecord("kafka-heartbeat", "kafka alive ?".getBytes(StandardCharsets.UTF_8)), new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (e != null) {
            logger.error("connect kafka error", e)
            connected = false
            Thread.sleep(1000)
            init
          } else {
            logger.info("connect kafka success")
            connected = true
          }
        }
      })
    } catch {
      case e: Exception =>
        logger.error("connect kafka error", e)
    }
  }

  def parseProducerConfig: Properties = {
    val props: Properties = new Properties
    props.put("bootstrap.servers", servers)
    props.put("acks", "0")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("max.block.ms", "90000")
    props.put("compression.type", "gzip")
    props
  }

  override def send(topic: String, data: Array[Byte]): Unit = {
    checkInitSuccess
    producer.send(new ProducerRecord(topic, data))
  }

  override def send(topic: String, data: Array[Byte], callback: Callback): Unit = {
    checkInitSuccess
    producer.send(new ProducerRecord(topic, data), callback)
  }

  private def checkInitSuccess: Unit = {
    if (producer == null) {
      try {
        Thread.sleep(200)
      } catch {
        case ex:Exception =>
          logger.error("consume", ex)
      }
      checkInitSuccess
    }
  }

  override def close: Unit = {
    if (producer != null) producer.close()
  }
}
