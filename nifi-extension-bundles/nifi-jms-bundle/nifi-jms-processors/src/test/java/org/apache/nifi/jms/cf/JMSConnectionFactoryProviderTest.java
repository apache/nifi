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
package org.apache.nifi.jms.cf;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link JMSConnectionFactoryProvider}
 */
public class JMSConnectionFactoryProviderTest {

    private static final String HOSTNAME = "myhost";
    private static final String PORT = "1234";

    private static final String SINGLE_TEST_BROKER = HOSTNAME + ":" + PORT;
    private static final String SINGLE_TEST_BROKER_WITH_SCHEME = "tcp://myhost:1234";
    private static final String SINGLE_TEST_BROKER_WITH_SCHEME_AND_IP = "tcp://0.0.0.0:616161";
    private static final String MULTIPLE_TEST_BROKERS = "myhost01:1234,myhost02:1234";
    private static final String SINGLE_ACTIVEMQ_BROKER = "tcp://myhost:61616";
    private static final String MULTIPLE_ACTIVEMQ_BROKERS = "failover:(tcp://myhost01:61616,tcp://myhost02:61616)?randomize=false";
    private static final String SINGLE_TIBCO_BROKER = "tcp://myhost:7222";
    private static final String MULTIPLE_TIBCO_BROKERS = "tcp://myhost01:7222,tcp://myhost02:7222";
    private static final String SINGLE_IBM_MQ_BROKER = "myhost(1414)";
    private static final String MULTIPLE_IBM_MQ_BROKERS = "myhost01(1414),myhost02(1414)";
    private static final String MULTIPLE_IBM_MQ_MIXED_BROKERS = "myhost01:1414,myhost02(1414)";
    private static final String MULTIPLE_IBM_MQ_COLON_PAIR_BROKERS = "myhost01:1414,myhost02:1414";
    private static final String SINGLE_QPID_JMS_BROKER = "amqp://myhost:5672";

    private static final String TEST_CONNECTION_FACTORY_IMPL = "org.apache.nifi.jms.testcflib.TestConnectionFactory";
    private static final String ACTIVEMQ_CONNECTION_FACTORY_IMPL = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String TIBCO_CONNECTION_FACTORY_IMPL = "com.tibco.tibjms.TibjmsConnectionFactory";
    private static final String IBM_MQ_CONNECTION_FACTORY_IMPL = "com.ibm.mq.jms.MQConnectionFactory";
    private static final String QPID_JMS_CONNECTION_FACTORY_IMPL = "org.apache.qpid.jms.JmsConnectionFactory";

    private static final String CF_PROVIDER_SERVICE_ID = "cfProvider";
    private static final String SSL_CONTEXT_SERVICE_ID = "sslContextService";

    private static final String DUMMY_JAR_1 = "dummy-lib.jar";
    private static final String DUMMY_JAR_2 = "dummy-lib-2.jar";
    private static final String DUMMY_CONF = "dummy.conf";

    private String dummyResource;
    private String allDummyResources;

    @BeforeEach
    public void prepareTest() throws URISyntaxException {
        dummyResource = this.getClass().getResource("/" + DUMMY_JAR_1).toURI().toString();
        allDummyResources = this.getClass().getResource("/" + DUMMY_JAR_1).toURI().toString() + "," +
                this.getClass().getResource("/" + DUMMY_JAR_2).toURI().toString() + "," +
                this.getClass().getResource("/" + DUMMY_CONF).toURI().toString() + ",";
    }

    @Test
    public void validateNotValidForNonExistingLibPath() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, "foo");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertNotValid(cfProvider);
    }

    @Test
    public void validateELExpression() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));
        runner.setValidateExpressionUsage(true);

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setEnvironmentVariableValue("broker.uri", SINGLE_TEST_BROKER_WITH_SCHEME_AND_IP);
        runner.setEnvironmentVariableValue("client.lib", dummyResource);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, "${broker.uri}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, "${client.lib}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void testClientLibResourcesLoaded() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));
        runner.setValidateExpressionUsage(true);

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setEnvironmentVariableValue("broker.uri", SINGLE_TEST_BROKER_WITH_SCHEME_AND_IP);
        runner.setEnvironmentVariableValue("client.lib", allDummyResources);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, "${broker.uri}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, "${client.lib}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);

        ClassLoader loader = runner.getClass().getClassLoader();
        assertNotNull(loader.getResource(DUMMY_CONF));
        assertNotNull(loader.getResource(DUMMY_JAR_1));
        assertNotNull(loader.getResource(DUMMY_JAR_2));
    }

    @Test
    public void validateGetConnectionFactoryFailureIfServiceNotConfigured() {
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider() {
            @Override
            protected ComponentLog getLogger() {
                return new MockComponentLog("cfProvider", this);
            }
        };
        cfProvider.onEnabled(new MockConfigurationContext(Collections.emptyMap(), null, null));
        assertThrows(IllegalStateException.class, cfProvider::getConnectionFactory);
    }

    @Test
    public void validWithSingleTestBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleTestBrokerWithScheme() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER_WITH_SCHEME);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleTestBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TEST_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleActiveMqBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_ACTIVEMQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleActiveMqBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_ACTIVEMQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleTibcoBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TIBCO_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleTibcoBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TIBCO_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleIbmMqBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_IBM_MQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleIbmMqBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleIbmMqMixedBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_MIXED_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleIbmMqColorPairBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_COLON_PAIR_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleQpidJmsBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_QPID_JMS_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, QPID_JMS_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void propertiesSetOnSingleTestBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("hostName", HOSTNAME, "port", PORT), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleTestBrokerWithSchemaConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER_WITH_SCHEME);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of(), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnMultipleTestBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TEST_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("hostName", "myhost01", "port", "1234"), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleActiveMqBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_ACTIVEMQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("brokerURL", SINGLE_ACTIVEMQ_BROKER), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnMultipleActiveMqBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_ACTIVEMQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("brokerURL", MULTIPLE_ACTIVEMQ_BROKERS), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleActiveMqBrokerWithSslConnectionFactory() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_ACTIVEMQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        String trustStoreFile = "/path/to/truststore";
        String trustStorePassword = "truststore_password";
        String trustStoreType = "JKS";
        String keyStoreFile = "/path/to/keystore";
        String keyStorePassword = "keystore_password";
        String keyPassword = "key_password";
        String keyStoreType = "PKCS12";

        SSLContextService sslContextService = mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_SERVICE_ID);
        when(sslContextService.isTrustStoreConfigured()).thenReturn(true);
        when(sslContextService.getTrustStoreFile()).thenReturn(trustStoreFile);
        when(sslContextService.getTrustStorePassword()).thenReturn(trustStorePassword);
        when(sslContextService.getTrustStoreType()).thenReturn(trustStoreType);
        when(sslContextService.isKeyStoreConfigured()).thenReturn(true);
        when(sslContextService.getKeyStoreFile()).thenReturn(keyStoreFile);
        when(sslContextService.getKeyStorePassword()).thenReturn(keyStorePassword);
        when(sslContextService.getKeyPassword()).thenReturn(keyPassword);
        when(sslContextService.getKeyStoreType()).thenReturn(keyStoreType);

        runner.addControllerService(SSL_CONTEXT_SERVICE_ID, sslContextService);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_ID);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of(
                        "brokerURL", SINGLE_ACTIVEMQ_BROKER,
                        "trustStore", trustStoreFile,
                        "trustStorePassword", trustStorePassword,
                        "trustStoreType", trustStoreType,
                        "keyStore", keyStoreFile,
                        "keyStorePassword", keyStorePassword,
                        "keyStoreKeyPassword", keyPassword,
                        "keyStoreType", keyStoreType
                        ),
                cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleTibcoBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TIBCO_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("serverUrl", SINGLE_TIBCO_BROKER), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnMultipleTibcoBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TIBCO_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("serverUrl", MULTIPLE_TIBCO_BROKERS), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleIbmMqBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_IBM_MQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("connectionNameList", SINGLE_IBM_MQ_BROKER), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnMultipleIbmMqBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("connectionNameList", MULTIPLE_IBM_MQ_BROKERS), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnMultipleIbmMqMixedBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_MIXED_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("connectionNameList", MULTIPLE_IBM_MQ_BROKERS), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnMultipleIbmMqColonPairBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_COLON_PAIR_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("connectionNameList", MULTIPLE_IBM_MQ_BROKERS), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleIbmMqColonSeparatedPairBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("connectionNameList", HOSTNAME + "(" + PORT + ")"), cfProvider.getConfiguredProperties());
    }

    @Test
    public void dynamicPropertiesSetOnSingleTestBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setEnvironmentVariableValue("test", "dynamicValue");

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);
        runner.setProperty(cfProvider, "dynamicProperty", "${test}");

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("dynamicProperty", "dynamicValue", "hostName", HOSTNAME, "port", PORT), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleQpidJmsConnectionFactory() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_QPID_JMS_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, QPID_JMS_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("remoteURI", SINGLE_QPID_JMS_BROKER), cfProvider.getConfiguredProperties());
    }

    @Test
    public void propertiesSetOnSingleQpidJmsWithSslConnectionFactory() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(CF_PROVIDER_SERVICE_ID, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_QPID_JMS_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, QPID_JMS_CONNECTION_FACTORY_IMPL);

        SSLContext sslContext = SSLContext.getDefault();
        SSLContextService sslContextService = mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_SERVICE_ID);
        when(sslContextService.createContext()).thenReturn(sslContext);

        runner.addControllerService(SSL_CONTEXT_SERVICE_ID, sslContextService);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_ID);

        runner.enableControllerService(cfProvider);

        assertEquals(Map.of("remoteURI", SINGLE_QPID_JMS_BROKER, "sslContext", sslContext), cfProvider.getConfiguredProperties());
    }
}
