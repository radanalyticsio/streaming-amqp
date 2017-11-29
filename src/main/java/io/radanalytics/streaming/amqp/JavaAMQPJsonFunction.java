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

package io.radanalytics.streaming.amqp;

import org.apache.qpid.proton.message.Message;
import org.apache.spark.api.java.function.Function;
import scala.Option;

/**
 * Provides implementation for a function which has an AMQP messages as input
 * and provide a JSON representation as output
 */
public class JavaAMQPJsonFunction implements Function<Message, Option<String>> {

    @Override
    public Option<String> call(Message message) throws Exception {

        AMQPJsonFunction function = new AMQPJsonFunction();
        return function.apply(message);
    }
}
