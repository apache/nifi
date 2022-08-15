/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.mqtt;

import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.MqttTestClient;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.processors.mqtt.common.TestPublishMqttCommon;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPublishMQTT extends TestPublishMqttCommon {

    @Override
    public void verifyPublishedMessage(byte[] payload, int qos, boolean retain) {
        StandardMqttMessage lastPublishedMessage = mqttTestClient.getLastPublishedMessage();
        String lastPublishedTopic = mqttTestClient.getLastPublishedTopic();
        assertEquals(Arrays.toString(payload), Arrays.toString(lastPublishedMessage.getPayload()));
        assertEquals(qos, lastPublishedMessage.getQos());
        assertEquals(retain, lastPublishedMessage.isRetained());
        assertEquals(topic, lastPublishedTopic);
    }

    private MqttTestClient mqttTestClient;

    public class UnitTestablePublishMqtt extends PublishMQTT {

        public UnitTestablePublishMqtt(){
            super();
        }

        @Override
        protected MqttClient createMqttClient() throws MqttException {
            mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
            return mqttTestClient;
        }
    }

    @BeforeEach
    public void init() {
        UnitTestablePublishMqtt proc = new UnitTestablePublishMqtt();
        testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PublishMQTT.PROP_BROKER_URI, "tcp://localhost:1883");
        testRunner.setProperty(PublishMQTT.PROP_RETAIN, "false");
        topic = "testTopic";
        testRunner.setProperty(PublishMQTT.PROP_TOPIC, topic);
    }
}
