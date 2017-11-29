/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.amqp;

import org.apache.qpid.proton.message.Message;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * Java test suite for the AMQP input stream
 */
public class JavaAMQPServerStreamSuite {

    private Duration batchDuration = new Duration(1000);
    private String master = "local[2]";
    private String appName = this.getClass().getSimpleName();
    private String address = "my_address";
    private String checkpointDir = "/tmp/spark-streaming-amqp-tests";

    private SparkConf conf = null;
    private JavaStreamingContext jssc = null;
    private AMQPTestUtils amqpTestUtils = null;

    @Before
    public void setup() {

        this.conf = new SparkConf().setMaster(this.master).setAppName(this.appName);
        conf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        this.jssc = new JavaStreamingContext(this.conf, this.batchDuration);
        this.jssc.checkpoint(checkpointDir);

        this.amqpTestUtils = new AMQPTestUtils();
        this.amqpTestUtils.setup();
    }

    @After
    public void teardown() {

        if (this.jssc != null) {
            this.jssc.stop();
        }

        if (this.amqpTestUtils != null) {
            this.amqpTestUtils.teardown();
        }
    }

    @Test
    public void testAMQPReceiveServer() {

        String sendMessage = "Spark Streaming & AMQP";
        int max = 10;
        long delay = 100;

        this.amqpTestUtils.startAMQPServer(sendMessage, max, delay);

        Function<Message, Option<String>> converter = new JavaAMQPBodyFunction<>();

        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.amqpTestUtils.username(),
                        this.amqpTestUtils.password(),
                        this.address, converter, StorageLevel.MEMORY_ONLY());

        List<String> receivedMessage = new ArrayList<>();
        receiveStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receivedMessage.addAll(rdd.collect());
            }
        });

        jssc.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receivedMessage.size() == max);

        jssc.stop();

        amqpTestUtils.stopAMQPServer();
    }

}
