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

import com.hivemq.embedded.EmbeddedHiveMQ;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardKeyManagerBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_MQTT_VERSION_311;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_MQTT_VERSION_500;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumeMQTTTLSTest {

    private static final String LOCALHOST = "localhost";

    private static final String KEY_ALGORITHM = "RSA";

    private static final X500Principal CERTIFICATE_PRINCIPAL = new X500Principal("CN=%s".formatted(LOCALHOST));

    private static final char[] BROKER_KEY_STORE_PASSWORD = UUID.randomUUID().toString().toCharArray();

    private static final char[] CLIENT_KEY_PASSWORD = UUID.randomUUID().toString().toCharArray();

    private static final Duration CERTIFICATE_VALIDITY = Duration.ofHours(1);

    private static final long BROKER_LIFECYCLE_TIMEOUT_SECONDS = 30;

    private static final long MESSAGE_TIMEOUT_SECONDS = 15;

    private static final long POLL_INTERVAL_MILLIS = 100;

    private static final String INTERNAL_QUEUE_SIZE = "100";

    private static final String CONNECTION_TIMEOUT_SECONDS = "10";

    private static final String TOPIC_MQTT_3 = "mqtt-3";

    private static final String TOPIC_MQTT_5 = "mqtt-5";

    @TempDir
    private static Path tempDir;

    private static EmbeddedHiveMQ embeddedHiveMQ;

    private static int tlsPort;

    private static SSLContext clientSslContext;

    private static X509TrustManager clientTrustManager;

    private static X509ExtendedKeyManager clientKeyManager;

    @BeforeAll
    static void startBroker() throws Exception {
        tlsPort = getAvailablePort();

        final KeyPair keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, CERTIFICATE_PRINCIPAL, CERTIFICATE_VALIDITY).build();

        final KeyStore ephemeralKeyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .keyPassword(CLIENT_KEY_PASSWORD)
                .build();
        clientSslContext = new StandardSslContextBuilder()
                .trustStore(ephemeralKeyStore)
                .keyStore(ephemeralKeyStore)
                .keyPassword(CLIENT_KEY_PASSWORD)
                .build();
        clientTrustManager = new StandardTrustManagerBuilder().trustStore(ephemeralKeyStore).build();
        clientKeyManager = new StandardKeyManagerBuilder().keyStore(ephemeralKeyStore).keyPassword(CLIENT_KEY_PASSWORD).build();

        final Path brokerKeyStorePath = writeBrokerKeyStore(keyPair, certificate);
        final Path configFolder = Files.createDirectories(tempDir.resolve("conf"));
        final Path dataFolder = Files.createDirectories(tempDir.resolve("data"));
        final Path extensionsFolder = Files.createDirectories(tempDir.resolve("extensions"));
        Files.writeString(configFolder.resolve("config.xml"), getBrokerConfiguration(brokerKeyStorePath));

        embeddedHiveMQ = EmbeddedHiveMQ.builder()
                .withConfigurationFolder(configFolder)
                .withDataFolder(dataFolder)
                .withExtensionsFolder(extensionsFolder)
                .withoutLoggingBootstrap()
                .build();
        embeddedHiveMQ.start().get(BROKER_LIFECYCLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (embeddedHiveMQ != null) {
            embeddedHiveMQ.stop().get(BROKER_LIFECYCLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            embeddedHiveMQ.close();
        }
    }

    @Test
    void testConsumeOverTlsUsingMqttVersion3() throws Exception {
        final TestRunner testRunner = createTestRunner(TOPIC_MQTT_3, ALLOWABLE_VALUE_MQTT_VERSION_311.getValue(), "ConsumeMQTTTLSTest-3");

        assertConsumesPublishedMessageOverTls(testRunner, TOPIC_MQTT_3);
    }

    @Test
    void testConsumeOverTlsUsingMqttVersion5() throws Exception {
        final TestRunner testRunner = createTestRunner(TOPIC_MQTT_5, ALLOWABLE_VALUE_MQTT_VERSION_500.getValue(), "ConsumeMQTTTLSTest-5");

        assertConsumesPublishedMessageOverTls(testRunner, TOPIC_MQTT_5);
    }

    private void assertConsumesPublishedMessageOverTls(final TestRunner testRunner, final String topic) throws Exception {
        // Establish the TLS connection and subscription
        testRunner.run(1, false, true);

        final String payload = "message-%s".formatted(topic);
        publishMessageOverTls(topic, payload);

        final List<MockFlowFile> flowFiles = awaitFlowFiles(testRunner);
        assertEquals(1, flowFiles.size());
        flowFiles.getFirst().assertContentEquals(payload);
    }

    private List<MockFlowFile> awaitFlowFiles(final TestRunner testRunner) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(MESSAGE_TIMEOUT_SECONDS);
        List<MockFlowFile> flowFiles;
        do {
            testRunner.run(1, false, false);
            flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
            if (!flowFiles.isEmpty()) {
                return flowFiles;
            }

            TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MILLIS);
        } while (System.currentTimeMillis() < deadline);
        return flowFiles;
    }

    private TestRunner createTestRunner(final String topic, final String mqttVersion, final String clientId) throws InitializationException {
        final TestRunner testRunner = TestRunners.newTestRunner(ConsumeMQTT.class);
        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, "ssl://%s:%d".formatted(LOCALHOST, tlsPort));
        testRunner.setProperty(ConsumeMQTT.PROP_CLIENTID, clientId);
        testRunner.setProperty(ConsumeMQTT.PROP_TOPIC_FILTER, topic);
        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, INTERNAL_QUEUE_SIZE);
        testRunner.setProperty(ConsumeMQTT.PROP_CONN_TIMEOUT, CONNECTION_TIMEOUT_SECONDS);
        testRunner.setProperty(ConsumeMQTT.PROP_MQTT_VERSION, mqttVersion);

        final String sslContextServiceIdentifier = "ssl-context-provider-%s".formatted(clientId);
        final SSLContextProvider sslContextProvider = mock(SSLContextProvider.class);
        when(sslContextProvider.getIdentifier()).thenReturn(sslContextServiceIdentifier);
        when(sslContextProvider.createContext()).thenReturn(clientSslContext);
        when(sslContextProvider.createTrustManager()).thenReturn(clientTrustManager);
        when(sslContextProvider.createKeyManager()).thenReturn(Optional.of(clientKeyManager));

        testRunner.addControllerService(sslContextServiceIdentifier, sslContextProvider);
        testRunner.enableControllerService(sslContextProvider);
        testRunner.setProperty(ConsumeMQTT.PROP_SSL_CONTEXT_SERVICE, sslContextServiceIdentifier);

        testRunner.assertValid();
        return testRunner;
    }

    private void publishMessageOverTls(final String topic, final String payload) throws Exception {
        final MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setSocketFactory(clientSslContext.getSocketFactory());

        final MqttClient publisher = new MqttClient(
                "ssl://%s:%d".formatted(LOCALHOST, tlsPort), "ConsumeMQTTTLSTest-Publisher-%s".formatted(topic), new MemoryPersistence());
        try {
            publisher.connect(connectOptions);
            final MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
            message.setQos(1);
            publisher.publish(topic, message);
        } finally {
            publisher.disconnect();
            publisher.close();
        }
    }

    private static Path writeBrokerKeyStore(final KeyPair keyPair, final X509Certificate certificate) throws Exception {
        final KeyStore brokerKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        brokerKeyStore.load(null);
        brokerKeyStore.setKeyEntry("broker", keyPair.getPrivate(), BROKER_KEY_STORE_PASSWORD, new Certificate[]{certificate});

        final Path keyStorePath = tempDir.resolve("broker-keystore");
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            brokerKeyStore.store(outputStream, BROKER_KEY_STORE_PASSWORD);
        }

        return keyStorePath;
    }

    private static String getBrokerConfiguration(final Path keyStorePath) {
        final String keyStorePassword = new String(BROKER_KEY_STORE_PASSWORD);
        return """
                <?xml version="1.0"?>
                <hivemq>
                    <listeners>
                        <tls-tcp-listener>
                            <port>%d</port>
                            <bind-address>0.0.0.0</bind-address>
                            <tls>
                                <keystore>
                                    <path>%s</path>
                                    <password>%s</password>
                                    <private-key-password>%s</private-key-password>
                                </keystore>
                                <truststore>
                                    <path>%s</path>
                                    <password>%s</password>
                                </truststore>
                                <client-authentication-mode>REQUIRED</client-authentication-mode>
                            </tls>
                        </tls-tcp-listener>
                    </listeners>
                </hivemq>
                """.formatted(tlsPort, keyStorePath, keyStorePassword, keyStorePassword, keyStorePath, keyStorePassword);
    }

    private static int getAvailablePort() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
