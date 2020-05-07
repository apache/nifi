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
package org.apache.nifi.jms.processors;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProperties;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties;
import org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * Tests for the different Connection Factory configurations of {@link PublishJMS} and {@link ConsumeJMS}:
 *     - JndiJmsConnectionFactoryProvider controller service
 *     - JMSConnectionFactoryProvider controller service
 *     - local JndiJmsConnectionFactory configuration on the processor
 *     - local JMSConnectionFactory configuration on the processor
 */
public class ConnectionFactoryConfigIT {

    private static final String CONTROLLER_SERVICE_ID = "cfProvider";

    private static final String BROKER_URL = "vm://test-broker?broker.persistent=false";

    private static final String PROP_JNDI_INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private static final String PROP_JNDI_PROVIDER_URL = BROKER_URL;
    private static final String PROP_JNDI_CONNECTION_FACTORY_NAME = "ConnectionFactory";

    private static final String PROP_JMS_CONNECTION_FACTORY_IMPL = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String PROP_JMS_BROKER_URI = BROKER_URL;

    private static final String TEST_MESSAGE = "test-message";

    private static Connection bootstrapConnection;

    private TestRunner publisher;
    private TestRunner consumer;

    @BeforeClass
    public static void beforeClass() throws JMSException {
        // start in-VM broker
        bootstrapConnection = new ActiveMQConnectionFactory(BROKER_URL).createConnection();
    }

    @AfterClass
    public static void afterClass() throws JMSException {
        // stop in-VM broker
        bootstrapConnection.close();
    }

    @Before
    public void before() {
        publisher = TestRunners.newTestRunner(PublishJMS.class);
        consumer = TestRunners.newTestRunner(ConsumeJMS.class);
    }

    @Test
    public void testJndiJmsConnectionFactoryControllerService() throws InitializationException {
        String queueName = "queue-jndi-service";

        configureJndiJmsConnectionFactoryControllerService(publisher, queueName);
        configureJndiJmsConnectionFactoryControllerService(consumer, queueName);

        executeProcessors();

        assertResult();
    }

    @Test
    public void testJMSConnectionFactoryControllerService() throws InitializationException {
        String queueName = "queue-jms-service";

        configureJMSConnectionFactoryControllerService(publisher, queueName);
        configureJMSConnectionFactoryControllerService(consumer, queueName);

        executeProcessors();

        assertResult();
    }

    @Test
    public void testLocalJndiJmsConnectionFactoryConfig() {
        String queueName = "queue-jndi-local";

        configureLocalJndiJmsConnectionFactory(publisher, queueName);
        configureLocalJndiJmsConnectionFactory(consumer, queueName);

        executeProcessors();

        assertResult();
    }

    @Test
    public void testLocalJMSConnectionFactoryConfig() {
        String queueName = "queue-jms-local";

        configureLocalJMSConnectionFactory(publisher, queueName);
        configureLocalJMSConnectionFactory(consumer, queueName);

        executeProcessors();

        assertResult();
    }

    private void configureJndiJmsConnectionFactoryControllerService(TestRunner runner, String queueName) throws InitializationException {
        JndiJmsConnectionFactoryProvider cfProvider = new JndiJmsConnectionFactoryProvider();
        runner.addControllerService(CONTROLLER_SERVICE_ID, cfProvider);
        runner.setProperty(cfProvider, JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, PROP_JNDI_INITIAL_CONTEXT_FACTORY);
        runner.setProperty(cfProvider, JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, PROP_JNDI_PROVIDER_URL);
        runner.setProperty(cfProvider, JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, PROP_JNDI_CONNECTION_FACTORY_NAME);
        runner.enableControllerService(cfProvider);
        runner.setProperty(AbstractJMSProcessor.CF_SERVICE, CONTROLLER_SERVICE_ID);
        runner.setProperty(AbstractJMSProcessor.DESTINATION, queueName);
    }

    private void configureJMSConnectionFactoryControllerService(TestRunner runner, String queueName) throws InitializationException {
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CONTROLLER_SERVICE_ID, cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, PROP_JMS_CONNECTION_FACTORY_IMPL);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_BROKER_URI, PROP_JMS_BROKER_URI);
        runner.enableControllerService(cfProvider);
        runner.setProperty(AbstractJMSProcessor.CF_SERVICE, CONTROLLER_SERVICE_ID);
        runner.setProperty(AbstractJMSProcessor.DESTINATION, queueName);
    }

    private void configureLocalJndiJmsConnectionFactory(TestRunner runner, String queueName) {
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, PROP_JNDI_INITIAL_CONTEXT_FACTORY);
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, PROP_JNDI_PROVIDER_URL);
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, PROP_JNDI_CONNECTION_FACTORY_NAME);
        runner.setProperty(AbstractJMSProcessor.DESTINATION, queueName);
    }

    private void configureLocalJMSConnectionFactory(TestRunner runner, String queueName) {
        runner.setProperty(JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, PROP_JMS_CONNECTION_FACTORY_IMPL);
        runner.setProperty(JMSConnectionFactoryProperties.JMS_BROKER_URI, PROP_JMS_BROKER_URI);
        runner.setProperty(AbstractJMSProcessor.DESTINATION, queueName);
    }

    private void executeProcessors() {
        publisher.enqueue(TEST_MESSAGE);
        publisher.run();

        consumer.run();
    }

    private void assertResult() {
        publisher.assertAllFlowFilesTransferred(PublishJMS.REL_SUCCESS, 1);
        publisher.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS).get(0).assertContentEquals(TEST_MESSAGE);

        consumer.assertAllFlowFilesTransferred(ConsumeJMS.REL_SUCCESS, 1);
        consumer.getFlowFilesForRelationship(ConsumeJMS.REL_SUCCESS).get(0).assertContentEquals(TEST_MESSAGE);
    }
}
