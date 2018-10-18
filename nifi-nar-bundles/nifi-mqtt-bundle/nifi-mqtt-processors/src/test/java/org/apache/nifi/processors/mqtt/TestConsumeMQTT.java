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

import io.moquette.proto.messages.PublishMessage;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.mqtt.common.MQTTQueueMessage;
import org.apache.nifi.processors.mqtt.common.MqttTestClient;
import org.apache.nifi.processors.mqtt.common.TestConsumeMqttCommon;
import org.apache.nifi.util.TestRunners;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestConsumeMQTT extends TestConsumeMqttCommon {
    public MqttTestClient mqttTestClient;

    public class UnitTestableConsumeMqtt extends ConsumeMQTT {

        public UnitTestableConsumeMqtt(){
            super();
        }

        @Override
        public IMqttClient createMqttClient(String broker, String clientID, MemoryPersistence persistence) throws MqttException {
            mqttTestClient =  new MqttTestClient(broker, clientID, MqttTestClient.ConnectType.Subscriber);
            return mqttTestClient;
        }
    }

    @Before
    public void init() throws IOException {
        PUBLISH_WAIT_MS = 0;

        broker = "tcp://localhost:1883";
        UnitTestableConsumeMqtt proc = new UnitTestableConsumeMqtt();
        testRunner = TestRunners.newTestRunner(proc);
        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, broker);
        testRunner.setProperty(ConsumeMQTT.PROP_CLIENTID, "TestClient");
        testRunner.setProperty(ConsumeMQTT.PROP_TOPIC_FILTER, "testTopic");
        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "100");
    }

    /**
     * If the session.commit() fails, we should not remove the unprocessed message
     */
    @Test
    public void testMessageNotConsumedOnCommitFail() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        testRunner.run(1, false);
        ConsumeMQTT processor = (ConsumeMQTT) testRunner.getProcessor();
        MQTTQueueMessage mock = mock(MQTTQueueMessage.class);
        when(mock.getPayload()).thenReturn(new byte[0]);
        when(mock.getTopic()).thenReturn("testTopic");
        BlockingQueue<MQTTQueueMessage> mqttQueue = getMqttQueue(processor);
        mqttQueue.add(mock);
        try {
            ProcessSession session = testRunner.getProcessSessionFactory().createSession();
            transferQueue(processor,
                    (ProcessSession) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { ProcessSession.class }, (proxy, method, args) -> {
                        if (method.getName().equals("commit")) {
                            throw new RuntimeException();
                        } else {
                            return method.invoke(session, args);
                        }
                    }));
            fail("Expected runtime exception");
        } catch (InvocationTargetException e) {
            assertTrue("Expected generic runtime exception, not " + e, e.getCause() instanceof RuntimeException);
        }
        assertTrue("Expected mqttQueue to contain uncommitted message.", mqttQueue.contains(mock));
    }

    @After
    public void tearDown() throws Exception {
        if (MQTT_server != null) {
            MQTT_server.stopServer();
        }
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

    @Override
    public void internalPublish(PublishMessage publishMessage) {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(publishMessage.getPayload().array());
        mqttMessage.setRetained(publishMessage.isRetainFlag());
        mqttMessage.setQos(publishMessage.getQos().ordinal());

        try {
            mqttTestClient.publish(publishMessage.getTopicName(), mqttMessage);
        } catch (MqttException e) {
            fail("Should never get an MqttException when publishing using test client");
        }
    }
}
