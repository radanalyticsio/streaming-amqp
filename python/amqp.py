#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.serializers import UTF8Deserializer
from pyspark.streaming import DStream

__all__ = ['AMQPUtils']


class AMQPUtils(object):

    @staticmethod
    def createStream(ssc, host, port, address):

        try:
            helper = ssc._jvm.org.apache.spark.streaming.amqp.AMQPUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                AMQPUtils._printErrorMsg(ssc.sparkContext)
            raise

        jstream = helper.createStream(ssc._jssc, host, port, address)
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def _printErrorMsg(sc):
        print("Spark Streaming's AMQP library not found in class path")
