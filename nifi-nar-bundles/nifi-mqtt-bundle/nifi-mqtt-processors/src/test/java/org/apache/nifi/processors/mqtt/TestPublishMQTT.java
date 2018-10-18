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

import org.apache.nifi.processors.mqtt.common.MQTTQueueMessage;
import org.apache.nifi.processors.mqtt.common.MqttTestClient;
import org.apache.nifi.processors.mqtt.common.TestPublishMqttCommon;
import org.apache.nifi.util.TestRunners;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class TestPublishMQTT extends TestPublishMqttCommon {

    @Override
    public void verifyPublishedMessage(byte[] payload, int qos, boolean retain) {
        MQTTQueueMessage mqttQueueMessage = mqttTestClient.publishedMessage;
        assertEquals(Arrays.toString(payload), Arrays.toString(mqttQueueMessage.getPayload()));
        assertEquals(qos, mqttQueueMessage.getQos());
        assertEquals(retain, mqttQueueMessage.isRetained());
        assertEquals(topic, mqttQueueMessage.getTopic());
    }


    public MqttTestClient mqttTestClient;

    public class UnitTestablePublishMqtt extends PublishMQTT {

        public UnitTestablePublishMqtt(){
            super();
        }

        @Override
        public IMqttClient createMqttClient(String broker, String clientID, MemoryPersistence persistence) throws MqttException {
            mqttTestClient =  new MqttTestClient(broker, clientID, MqttTestClient.ConnectType.Publisher);
            return mqttTestClient;
        }
    }

    @Before
    public void init() throws IOException {
        UnitTestablePublishMqtt proc = new UnitTestablePublishMqtt();
        testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(PublishMQTT.PROP_BROKER_URI, "tcp://localhost:1883");
        testRunner.setProperty(PublishMQTT.PROP_CLIENTID, "TestClient");
        testRunner.setProperty(PublishMQTT.PROP_RETAIN, "false");
        topic = "testTopic";
        testRunner.setProperty(PublishMQTT.PROP_TOPIC, topic);
    }

    @After
    public void tearDown() throws Exception {
        final File folder =  new File("./target");
        final File[] files = folder.listFiles( new FilenameFilter() {
            @Override
            public boolean accept( final File dir,
                                   final String name ) {
                return name.matches( "moquette_store.mapdb.*" );
            }
        } );
        for ( final File file : files ) {
            if ( !file.delete() ) {
                System.err.println( "Can't remove " + file.getAbsolutePath() );
            }
        }
    }
}
