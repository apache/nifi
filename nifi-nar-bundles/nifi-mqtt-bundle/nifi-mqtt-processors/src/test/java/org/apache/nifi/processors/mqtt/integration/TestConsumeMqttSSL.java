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
import io.moquette.proto.messages.AbstractMessage;
import io.moquette.proto.messages.PublishMessage;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.apache.nifi.processors.mqtt.ConsumeMQTT;
import org.apache.nifi.processors.mqtt.common.TestConsumeMqttCommon;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.common.MqttTestUtils.createSslProperties;


public class TestConsumeMqttSSL extends TestConsumeMqttCommon {


    private void startServer() throws IOException {
        MQTT_server = new Server();
        final Properties configProps = new Properties();

        configProps.put(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, "1884");
        configProps.put(BrokerConstants.SSL_PORT_PROPERTY_NAME, "8883");
        configProps.put(BrokerConstants.JKS_PATH_PROPERTY_NAME, "src/test/resources/keystore.jks");
        configProps.put(BrokerConstants.KEY_STORE_PASSWORD_PROPERTY_NAME, "passwordpassword");
        configProps.put(BrokerConstants.KEY_MANAGER_PASSWORD_PROPERTY_NAME, "passwordpassword");
        configProps.setProperty(PERSISTENT_STORE_PROPERTY_NAME,"./target/moquette_store.mapdb");
        IConfig server_config = new MemoryConfig(configProps);
        MQTT_server.startServer(server_config);
    }

    @Before
    public void init() throws IOException, InitializationException {
        startServer();

        broker = "ssl://localhost:8883";
        testRunner = TestRunners.newTestRunner(ConsumeMQTT.class);
        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, broker);
        testRunner.setProperty(ConsumeMQTT.PROP_CLIENTID, "TestClient");
        testRunner.setProperty(ConsumeMQTT.PROP_TOPIC_FILTER, "testTopic");
        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "100");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        Map<String, String> sslProperties = createSslProperties();
        testRunner.addControllerService("ssl-context", sslService, sslProperties);
        testRunner.enableControllerService(sslService);
        testRunner.setProperty(ConsumeMQTT.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
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
    public void testRetainedQoS2() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(true);

        internalPublish(testMessage);

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "true");
    }

    @Override
    public void internalPublish(PublishMessage publishMessage) {
        MQTT_server.internalPublish(publishMessage);
    }
}
