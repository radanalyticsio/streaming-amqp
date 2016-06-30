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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Java test suite for the AMQP input stream
 */
public class JavaAMQPStreamSuite {

    private Duration batchDuration = new Duration(1000);
    private String master = "local[2]";
    private String appName = this.getClass().getSimpleName();
    private String address = "my_address";

    private SparkConf conf = null;
    private JavaStreamingContext jssc = null;
    private AMQPTestUtils amqpTestUtils = null;

    @Before
    public void setup() {

        this.conf = new SparkConf().setMaster(this.master).setAppName(this.appName);
        this.jssc = new JavaStreamingContext(this.conf, this.batchDuration);

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
    public void testAMQPReceiveSimpleBodyString() {

        this.amqpTestUtils.startBroker();

        Function f = new AMQPFunction<String>();

        String sendMessage = "Spark Streaming & AMQP";
        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.address, f, StorageLevel.MEMORY_ONLY());

        List<String> receiveMessage = new ArrayList<>();
        receiveStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receiveMessage.add(rdd.first());
            }
        });

        jssc.start();

        this.amqpTestUtils.sendSimpleMessage(address, sendMessage);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receiveMessage.get(0).equals(sendMessage));

        jssc.stop();

        this.amqpTestUtils.stopBroker();
    }

    @Test
    public void testAMQPReceiveServer() {

        String sendMessage = "Spark Streaming & AMQP";
        int max = 10;

        this.amqpTestUtils.startAMQPServer(this.address, sendMessage, max);

        Function f = new AMQPFunction<String>();

        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.address, f, StorageLevel.MEMORY_ONLY());

        List<String> receiveMessage = new ArrayList<>();
        receiveStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receiveMessage.addAll(rdd.collect());
            }
        });

        jssc.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receiveMessage.size() == max);

        jssc.stop();

        amqpTestUtils.stopAMQPServer();
    }
}
