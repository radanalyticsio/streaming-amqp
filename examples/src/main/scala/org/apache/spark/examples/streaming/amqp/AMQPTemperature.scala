package org.apache.spark.examples.streaming.amqp

import java.lang.Long

import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.proton._
import org.apache.log4j.{Level, Logger}
import org.apache.qpid.proton.amqp.messaging.{AmqpValue, Section}
import org.apache.qpid.proton.message.Message
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.util.Random

/**
  * Sample application for getting insights from published temperature values
  */
object AMQPTemperature {

  private val master: String = "local[2]"
  private val appName: String = getClass().getSimpleName()

  private val batchDuration: Duration = Seconds(1)

  private val host: String = "localhost"
  private val port: Int = 5672
  private val address: String = "temperature"

  def main(args: Array[String]): Unit = {

    def messageConverter: Message => Option[Int] = {

      case message => {
        val body: Section = message.getBody()
        if (body.isInstanceOf[AmqpValue]) {
          val temp: Int = body.asInstanceOf[AmqpValue].getValue().asInstanceOf[Int]
          Some(temp)
        } else {
          None
        }
      }
      case _ =>
        None
    }

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, batchDuration)

    val receiveStream = AMQPUtils.createStream(ssc, host, port, address, messageConverter, StorageLevel.MEMORY_ONLY)

    // get maximum temperature in a window
    val max = receiveStream.reduceByWindow((a,b) => if (a > b) a else b, Seconds(5), Seconds(5))

    max.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * Sample application which publishes temperature values to an AMQP node
  */
object AMQPPublisher {

  private val host: String = "localhost"
  private val port: Int = 5672
  private val address: String = "temperature"

  def main(args: Array[String]): Unit = {

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val vertx: Vertx = Vertx.vertx()

    val client:ProtonClient = ProtonClient.create(vertx)

    client.connect(host, port, new Handler[AsyncResult[ProtonConnection]] {
      override def handle(ar: AsyncResult[ProtonConnection]): Unit = {
        if (ar.succeeded()) {

          val connection: ProtonConnection = ar.result()
          connection.open()

          val sender: ProtonSender = connection.createSender(address)
          sender.open()

          val random = new Random()

          vertx.setPeriodic(1000, new Handler[Long] {
            override def handle(timer: Long): Unit = {

              val temp: Int = 20 + random.nextInt(5)

              val message: Message = ProtonHelper.message()
              message.setBody(new AmqpValue(temp))

              println("Temperature = " + temp)
              sender.send(message, new Handler[ProtonDelivery] {
                override def handle(delivery: ProtonDelivery): Unit = {

                }
              })
            }
          })

        }
      }
    })

    System.in.read()
  }
}
