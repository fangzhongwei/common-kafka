package com.jxjxgo.common.mq.service

import scala.collection.mutable.ListBuffer

/**
  * Created by fangzhongwei on 2017/2/9.
  */
trait ConsumerService {
  def consume(list: ListBuffer[Array[Byte]]): Unit
}
