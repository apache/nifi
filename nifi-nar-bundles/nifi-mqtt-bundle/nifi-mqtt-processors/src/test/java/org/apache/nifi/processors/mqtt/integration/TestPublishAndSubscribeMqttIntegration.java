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

package org.apache.nifi.processors.mqtt.integration;

import io.moquette.BrokerConstants;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.apache.nifi.processors.mqtt.ConsumeMQTT;
import org.apache.nifi.processors.mqtt.PublishMQTT;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.PublishMQTT.REL_SUCCESS;
import static org.apache.nifi.processors.mqtt.common.TestConsumeMqttCommon.reconnect;

public class TestPublishAndSubscribeMqttIntegration {
    private TestRunner testSubscribeRunner;
    private TestRunner testPublishRunner;
    private Server MQTT_server;

    private static int PUBLISH_WAIT_MS = 1000;

    private void startServer() throws IOException {
        MQTT_server = new Server();
        final Properties configProps = new Properties();
        configProps.put(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, "1884");
        configProps.setProperty(PERSISTENT_STORE_PROPERTY_NAME,"./target/moquette_store.mapdb");
        IConfig server_config = new MemoryConfig(configProps);
        MQTT_server.startServer(server_config);
    }

    @Before
    public void init() throws IOException {
        startServer();
        testSubscribeRunner = TestRunners.newTestRunner(ConsumeMQTT.class);
        testPublishRunner = TestRunners.newTestRunner(PublishMQTT.class);
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

    @Test
    public void testBasic() throws Exception {
        subscribe();
        publishAndVerify();
        Thread.sleep(PUBLISH_WAIT_MS);
        testSubscribeRunner.run();
        subscribeVerify();
    }

    private void publishAndVerify(){
        testPublishRunner.setProperty(PublishMQTT.PROP_BROKER_URI, "tcp://localhost:1883");
        testPublishRunner.setProperty(PublishMQTT.PROP_CLIENTID, "TestPublishClient");
        testPublishRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testPublishRunner.setProperty(PublishMQTT.PROP_RETAIN, "false");
        testPublishRunner.setProperty(PublishMQTT.PROP_TOPIC, "testTopic");

        testPublishRunner.assertValid();

        String testMessage = "testMessage";
        testPublishRunner.enqueue(testMessage.getBytes());

        testPublishRunner.run();

        testPublishRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        testPublishRunner.assertTransferCount(REL_SUCCESS, 1);
    }

    private void subscribe() throws IOException, ClassNotFoundException, MqttException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, NoSuchFieldException {
        testSubscribeRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, "tcp://localhost:1883");
        testSubscribeRunner.setProperty(ConsumeMQTT.PROP_CLIENTID, "TestSubscribeClient");
        testSubscribeRunner.setProperty(ConsumeMQTT.PROP_TOPIC_FILTER, "testTopic");
        testSubscribeRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");
        testSubscribeRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "100");

        testSubscribeRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testSubscribeRunner.getProcessor();
        consumeMQTT.onScheduled(testSubscribeRunner.getProcessContext());
        reconnect(consumeMQTT, testSubscribeRunner.getProcessContext());
    }

    private void subscribeVerify(){
        testSubscribeRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);

        List<MockFlowFile> flowFiles = testSubscribeRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, "tcp://localhost:1883");
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }
}
