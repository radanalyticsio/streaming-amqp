package org.apache.spark.streaming.amqp

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}
import io.vertx.proton.ProtonDelivery
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging

/**
  * Provides message rate control with related throttling
  * @param maxRateLimit       Max rate for receiving messages
  */
class AMQPRateController(
      maxRateLimit: Long
      ) extends Logging {

  private var throttling: Boolean = false

  private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

  final def getCurrentLimit: Long = rateLimiter.getRate.toLong

  /**
    * Initialization method
    */
  final def init(): Unit = {

    doInit()
  }

  /**
    * Try to acquire the permit to handle incoming message with related delivery
    * @param delivery       Delivery information
    * @param message        AMQP message received
    */
  final def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    // try to acquire the rate limiter in order to have permits at current rate
    if (rateLimiter.tryAcquire()) {

      if (throttling) {
        logInfo("Throttling ended ... ")
        throttling = false
        doThrottlingEnd(delivery, message)
      }

      doAcquire(delivery, message)

    // permit not acquired, max rate exceeded
    } else {

      if (!throttling) {
        // throttling start now
        throttling = true
        doThrottlingStart(delivery, message)
        logWarning("Throttling started ... ")
      }

      if (throttling) {

        logError("Throttling ... ")
        // already in throttling
        doThrottling(delivery, message)
      }

    }

  }

  def doInit(): Unit = { }

  def doAcquire(delivery: ProtonDelivery, message: Message): Unit = { }

  def doThrottlingStart(delivery: ProtonDelivery, message: Message): Unit = { }

  def doThrottlingEnd(delivery: ProtonDelivery, message: Message): Unit = { }

  def doThrottling(delivery: ProtonDelivery, message: Message): Unit = { }
}
