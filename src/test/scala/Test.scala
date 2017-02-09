import com.jxjxgo.common.kafka.template.{ConsumerTemplateImpl, ProducerTemplateImpl}
import com.jxjxgo.common.mq.service.ConsumerService

import scala.collection.mutable.ListBuffer

/**
  * Created by fangzhongwei on 2017/2/9.
  */
object Test {
  def main(args: Array[String]): Unit = {
    new ConsumerTemplateImpl("192.168.181.142:9092", "join.game", "game.type.10", new ConsumerService {
      override def consume(list: ListBuffer[Array[Byte]]): Unit = {
        list.foreach {
          r => println(s"value: ${new String(r)}")
        }
      }
    }).init

    val impl: ProducerTemplateImpl = new ProducerTemplateImpl("192.168.181.142:9092")
    impl.init()
    Thread.sleep(3000)// wait init finish
    impl.send("game.type.10", "i am joining game.".getBytes())
    println("send success... print enter to quit.")

    scala.io.StdIn.readLine()
  }
}
