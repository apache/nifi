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

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link JMSConnectionFactoryProvider}
 */
public class JMSConnectionFactoryProviderTest {

    private static Logger logger = LoggerFactory.getLogger(JMSConnectionFactoryProviderTest.class);

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

    private static final String TEST_CONNECTION_FACTORY_IMPL = "org.apache.nifi.jms.testcflib.TestConnectionFactory";
    private static final String ACTIVEMQ_CONNECTION_FACTORY_IMPL = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String TIBCO_CONNECTION_FACTORY_IMPL = "com.tibco.tibjms.TibjmsConnectionFactory";
    private static final String IBM_MQ_CONNECTION_FACTORY_IMPL = "com.ibm.mq.jms.MQConnectionFactory";

    private static final String controllerServiceId = "cfProvider";

    private static final String DUMMY_JAR_1 = "dummy-lib.jar";
    private static final String DUMMY_JAR_2 = "dummy-lib-2.jar";
    private static final String DUMMY_CONF = "dummy.conf";

    private String dummyResource;
    private String allDummyResources;

    @Before
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
        runner.addControllerService(controllerServiceId, cfProvider);

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
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setVariable("broker.uri", SINGLE_TEST_BROKER_WITH_SCHEME_AND_IP);
        runner.setVariable("client.lib", dummyResource);

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
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setVariable("broker.uri", SINGLE_TEST_BROKER_WITH_SCHEME_AND_IP);
        runner.setVariable("client.lib", allDummyResources);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, "${broker.uri}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, "${client.lib}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);

        ClassLoader loader = runner.getClass().getClassLoader();
        Assert.assertNotNull(loader.getResource(DUMMY_CONF));
        Assert.assertNotNull(loader.getResource(DUMMY_JAR_1));
        Assert.assertNotNull(loader.getResource(DUMMY_JAR_2));
    }

    @Test(expected = IllegalStateException.class)
    public void validateGetConnectionFactoryFailureIfServiceNotConfigured() {
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider() {
            @Override
            protected ComponentLog getLogger() {
                return new MockComponentLog("cfProvider", this);
            }
        };
        cfProvider.onEnabled(new MockConfigurationContext(Collections.emptyMap(), null));
        cfProvider.getConnectionFactory();
    }

    @Test
    public void validWithSingleTestBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleTestBrokerWithScheme() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER_WITH_SCHEME);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleTestBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TEST_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleActiveMqBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_ACTIVEMQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleActiveMqBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_ACTIVEMQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleTibcoBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TIBCO_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleTibcoBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TIBCO_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithSingleIbmMqBroker() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_IBM_MQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleIbmMqBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleIbmMqMixedBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_MIXED_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void validWithMultipleIbmMqColorPairBrokers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_COLON_PAIR_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.assertValid(cfProvider);
    }

    @Test
    public void propertiesSetOnSingleTestBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("hostName", HOSTNAME, "port", PORT));
    }

    @Test
    public void propertiesSetOnSingleTestBrokerWithSchemaConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER_WITH_SCHEME);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of());
    }

    @Test
    public void propertiesSetOnMultipleTestBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TEST_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("hostName", "myhost01", "port", "1234"));
    }

    @Test
    public void propertiesSetOnSingleActiveMqBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_ACTIVEMQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("brokerURL", SINGLE_ACTIVEMQ_BROKER));
    }

    @Test
    public void propertiesSetOnMultipleActiveMqBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_ACTIVEMQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, ACTIVEMQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("brokerURL", MULTIPLE_ACTIVEMQ_BROKERS));
    }

    @Test
    public void propertiesSetOnSingleTibcoBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TIBCO_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("serverUrl", SINGLE_TIBCO_BROKER));
    }

    @Test
    public void propertiesSetOnMultipleTibcoBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_TIBCO_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TIBCO_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("serverUrl", MULTIPLE_TIBCO_BROKERS));
    }

    @Test
    public void propertiesSetOnSingleIbmMqBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_IBM_MQ_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("connectionNameList", SINGLE_IBM_MQ_BROKER));
    }

    @Test
    public void propertiesSetOnMultipleIbmMqBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("connectionNameList", MULTIPLE_IBM_MQ_BROKERS));
    }

    @Test
    public void propertiesSetOnMultipleIbmMqMixedBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_MIXED_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("connectionNameList", MULTIPLE_IBM_MQ_BROKERS));
    }

    @Test
    public void propertiesSetOnMultipleIbmMqColonPairBrokersConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, MULTIPLE_IBM_MQ_COLON_PAIR_BROKERS);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("connectionNameList", MULTIPLE_IBM_MQ_BROKERS));
    }

    @Test
    public void propertiesSetOnSingleIbmMqColonSeparatedPairBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, IBM_MQ_CONNECTION_FACTORY_IMPL);

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("connectionNameList", HOSTNAME + "(" + PORT + ")"));
    }

    @Test
    public void dynamicPropertiesSetOnSingleTestBrokerConnectionFactory() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));

        JMSConnectionFactoryProviderForTest cfProvider = new JMSConnectionFactoryProviderForTest();
        runner.addControllerService(controllerServiceId, cfProvider);

        runner.setVariable("test", "dynamicValue");

        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, SINGLE_TEST_BROKER);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES, dummyResource);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, TEST_CONNECTION_FACTORY_IMPL);
        runner.setProperty(cfProvider, "dynamicProperty", "${test}");

        runner.enableControllerService(cfProvider);

        assertEquals(cfProvider.getConfiguredProperties(), ImmutableMap.of("dynamicProperty", "dynamicValue", "hostName", HOSTNAME, "port", PORT));
    }
}
