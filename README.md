[![Build Status](https://travis-ci.org/radanalyticsio/streaming-amqp.svg?branch=master)](https://travis-ci.org/radanalyticsio/streaming-amqp)

# AMQP connector for Spark Streaming

This project provides an [AMQP](https://www.amqp.org/) (Advanced Message Queuing Protocol) connector for [Apache Spark Streaming](http://spark.apache.org/streaming/) in order to ingest data as a stream from all possible AMQP based sources like :

* a pure AMQP server, which exposes a sender node for sending messages in a peer to peer fashion with the connector
* a messaging broker, which supports AMQP protocol and provide "store and forward" mechanism from queues and topics/subscriptions for the connector
* a router network, which provides AMQP routing with direct messaging or link routing

The implementation offers the following receivers :

* a non reliable receiver which doesn't settle the messages received from the AMQP sender
* a reliable receiver which settles messages received from the AMQP sender only after storing them reliably in the Spark cluster (it uses the checkpoint and write ahead log features)

The stream doesn't provide the received AMQP messages directly as elements of the RDDs micro batches but from the driver it's possible to pass a converter function in order to convert each message in the desidered format; it will be the type of the elements inside the RDDs micro batches. Two built in message converter functions are provided (as sample) :

* _AMQPBodyFunction[T]_ : a converter which returns only the AMQP message body in a custom serializable type T
* _AMQPJsonFunction_ : a converter which returns the JSON string representation of the entire AMQP message

## Project References

Using **Maven**

```
<dependency>
    <groupId>io.radanalytics</groupId>
    <artifactId>spark-streaming-amqp_2.11</artifactId>
    <version>0.3.0</version>
</dependency>
```

Using **SBT**

```
libraryDependencies += "io.radanalytics" %% "spark-streaming-amqp" % "0.3.0"
```

The library can be added to a Spark job launched through `spark-shell` or `spark-submit` using the `--packages` or `--jars` command line options. In order to use the `--packages` option, the library needs to be installed into the local repository.

```
bin/spark-shell --packages io.radanalytics:spark-streaming-amqp_2.11:0.3.0
```
> About installing package in the local repository, the `mvn clean install` command (for Maven) or the `sbt publish` (for SBT) need to be used.

## Receivers

The AMQP receiver is started using the **_AMQPUtils.createStream_** method which returns an _InputDStream_ and needs following parameters :

* **ssc** : instance of a _StreamingContext_
* **host** : hostname or IP address of the remote AMQP node to connect
* **port** : port of the remote AMQP node to connect
* **address** : AMQP address for which starting to receive messages
* **messageConverter** : a callback function which is called for every received AMQP message for converting it in the user desidered format that will be stored into the RDDs micro batches. It gets a Proton _Message_ instance as input and must returns an _Option[T]_ where _T_ is the serializable desired type by the user
* **storageLevel** : Spark storage level to use

Using default Spark configuration, a _non reliable_ receiver is started. In order to use the _reliable_ version, the WAL (Write Ahead Logs) and checkpoing must be enabled in the driver application. The WAL is enabled setting the following configuration parameter to _true_ :

```
spark.streaming.receiver.writeAheadLog.enable
```

### Scala

```scala
val converter = new AMQPBodyFunction[String]

val receiveStream = AMQPUtils.createStream(ssc,
                host, port, address,
                converter, StorageLevel.MEMORY_ONLY)
```

### Java

```java
Function converter = new JavaAMQPBodyFunction<String>();

String sendMessage = "Spark Streaming & AMQP";
JavaReceiverInputDStream<String>  receiveStream =
        AMQPUtils.createStream(this.jssc,
                this.host,
                this.port,
                this.username,
                this.password,
                this.address, converter, StorageLevel.MEMORY_ONLY());
```

### Python

The Python API leverages on the JSON converter and the RDDs micro batches always contain a String with the JSON representation of the received AMQP message.

```python
receiveStream = AMQPUtils.createStream(ssc, host, port, address)
```

## Example

The Scala example provided with the current project is related to a simple IoT scenario where the AMQP receiver gets temperature values from a _temperature_ address. It could be the name of a queue on a broker or a direct address inside a router network where a device is sending data.

The following message converter function is used, in order to estract the temperature value as an _Int_ from the AMQP message body.

```scala
def messageConverter(message: Message): Option[Int] = {
  message.getBody match {
      case body: Data => {
        val temp: Int = new String(body.getValue.getArray).toInt
        Some(temp)
      }
      case body: AmqpValue => {
        val temp: Int = body.asInstanceOf[AmqpValue].getValue.asInstanceOf[String].toInt
        Some(temp)
      }
      case _ => None
  }
}
```

The input stream returned by the AMQP receiver is processed with the _reduceByWindow_ method in order to get the maximum temperature value in a sliding window (5 seconds on top of a batch interval of 1 second).

```scala
val receiveStream = AMQPUtils.createStream(ssc, host, port, username, password, address, messageConverter _, StorageLevel.MEMORY_ONLY)

// get maximum temperature in a window
val max = receiveStream.reduceByWindow((a,b) => if (a > b) a else b, Seconds(5), Seconds(5))

max.print()
```

The full source code is available in the examples folder with the same version in Python.
