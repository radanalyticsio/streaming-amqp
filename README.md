# AMQP connector for Spark Streaming

This project provides an [AMQP](https://www.amqp.org/) (Advanced Message Queuing Protocol) connector for [Apache Spark Streaming](http://spark.apache.org/streaming/) in order to ingest data as a stream from all possible AMQP based sources like :

* a pure AMQP server, which exposes a sender node for sending messages in a peer to peer fashion with the connector
* a messaging broker, which supports AMQP protocol and provide "store and forward" mechanism from queues and topics/subscriptions for the connector
* a router network, which provides AMQP routing with direct messaging or link routing

The implementation offers the following receivers :

* a non reliable receiver which doesn't settle the messages received from the AMQP sender
* a reliable receiver which settles messages received from the AMQP sender only after storing them reliably in the Spark cluster (it uses the checkpoint and write ahead log features)

The stream doesn't provide the received AMQP messages directly as elements of the RDDs micro batches but from the driver it's possible to pass a converter function in order to convert each message in the desidered format; it will be the type of the elements inside the RDDs micro batches. Two built in message converter functions are provided (as sample) :

* a converter which returns only the AMQP message body in a custom serializable type T
* a converter which returns the JSON string representation of the entire AMQP message

## Project References

### Maven

```
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-amqp_2.11</artifactId>
    <version>0.1.0</version>
</dependency>
```

### SBT

```
libraryDependencies += "org.apache.spark" %% "spark-streaming-amqp_2.11" % "0.1.0"
```

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

TBD

### Java

TBD

## Example

TBD
