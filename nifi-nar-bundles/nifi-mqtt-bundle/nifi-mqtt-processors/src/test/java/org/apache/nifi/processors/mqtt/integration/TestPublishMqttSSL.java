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
import org.apache.nifi.processors.mqtt.PublishMQTT;
import org.apache.nifi.processors.mqtt.common.TestPublishMqttCommon;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static io.moquette.BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME;
import static org.apache.nifi.processors.mqtt.common.MqttTestUtils.createSslProperties;


public class TestPublishMqttSSL extends TestPublishMqttCommon {

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
        testRunner = TestRunners.newTestRunner(PublishMQTT.class);
        testRunner.setProperty(PublishMQTT.PROP_BROKER_URI, "ssl://localhost:8883");
        testRunner.setProperty(PublishMQTT.PROP_CLIENTID, "TestClient");
        testRunner.setProperty(PublishMQTT.PROP_RETAIN, "true");
        testRunner.setProperty(PublishMQTT.PROP_TOPIC, "testTopic");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        Map<String, String> sslProperties = createSslProperties();
        testRunner.addControllerService("ssl-context", sslService, sslProperties);
        testRunner.enableControllerService(sslService);
        testRunner.setProperty(PublishMQTT.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
    }

    @After
    public void tearDown() throws Exception {
        if (MQTT_server != null) {
            MQTT_server.stopServer();
        }
        final File folder = new File("./target");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir,
                                  final String name) {
                return name.matches("moquette_store.mapdb.*");
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
    }

    @Override
    public void verifyPublishedMessage(byte[] payload, int qos, boolean retain) {
        //Cannot verify published message without subscribing and consuming it which is outside the scope of this test.
    }
}
