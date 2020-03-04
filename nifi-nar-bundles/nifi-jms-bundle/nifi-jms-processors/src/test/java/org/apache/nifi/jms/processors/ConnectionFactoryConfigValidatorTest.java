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

import org.apache.nifi.jms.cf.JMSConnectionFactoryProperties;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link AbstractJMSProcessor.ConnectionFactoryConfigValidator}
 */
public class ConnectionFactoryConfigValidatorTest {

    private static final String CONTROLLER_SERVICE_ID = "cfProvider";

    private static final String PROP_JNDI_INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private static final String PROP_JNDI_PROVIDER_URL = "tcp://myhost:61616";
    private static final String PROP_JNDI_CONNECTION_FACTORY_NAME = "ConnectionFactory";

    private static final String PROP_JMS_CONNECTION_FACTORY_IMPL = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String PROP_JMS_BROKER_URI = "tcp://myhost:61616";

    private TestRunner runner;

    @Before
    public void setUp() {
        runner = TestRunners.newTestRunner(PublishJMS.class);
        runner.setProperty(PublishJMS.DESTINATION, "myQueue");
    }

    @Test
    public void testNotValidWhenNoConnectionFactoryConfigured() {
        runner.assertNotValid();
    }

    @Test
    public void testValidControllerServiceConfig() throws InitializationException {
        configureControllerService();

        runner.assertValid();
    }

    @Test
    public void testNotValidWhenControllerServiceConfiguredButLocalJndiJmsConnectionFactoryPropertyAlsoSpecified() throws InitializationException {
        configureControllerService();

        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, PROP_JNDI_CONNECTION_FACTORY_NAME);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenControllerServiceConfiguredButLocalJMSConnectionFactoryPropertyAlsoSpecified() throws InitializationException {
        configureControllerService();

        runner.setProperty(JMSConnectionFactoryProperties.JMS_BROKER_URI, PROP_JMS_BROKER_URI);

        runner.assertNotValid();
    }

    @Test
    public void testValidLocalJndiJmsConnectionFactoryConfig() {
        configureLocalJndiJmsConnectionFactory();

        runner.assertValid();
    }

    @Test
    public void testNotValidWhenLocalJndiJmsConnectionFactoryConfiguredButLocalJMSConnectionFactoryPropertyAlsoSpecified() {
        configureLocalJndiJmsConnectionFactory();

        runner.setProperty(JMSConnectionFactoryProperties.JMS_BROKER_URI, PROP_JMS_BROKER_URI);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenNoProviderUrlSpecifiedForLocalJndiJmsConnectionFactory() {
        configureLocalJndiJmsConnectionFactory();
        runner.removeProperty(JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWhenNoConnectionFactoryNameSpecifiedForLocalJndiJmsConnectionFactory() {
        configureLocalJndiJmsConnectionFactory();
        runner.removeProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME);

        runner.assertNotValid();
    }

    @Test
    public void testValidLocalJMSConnectionFactory() {
        configureLocalJMSConnectionFactory();

        runner.assertValid();
    }

    @Test
    public void testNotValidWhenLocalJMSConnectionFactoryConfiguredButLocalJndiJmsConnectionFactoryPropertyAlsoSpecified() {
        configureLocalJMSConnectionFactory();

        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, PROP_JNDI_CONNECTION_FACTORY_NAME);

        runner.assertNotValid();
    }

    private void configureControllerService() throws InitializationException {
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService(CONTROLLER_SERVICE_ID, cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, PROP_JMS_CONNECTION_FACTORY_IMPL);
        runner.enableControllerService(cfProvider);
        runner.setProperty(AbstractJMSProcessor.CF_SERVICE, CONTROLLER_SERVICE_ID);
    }

    private void configureLocalJndiJmsConnectionFactory() {
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, PROP_JNDI_INITIAL_CONTEXT_FACTORY);
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, PROP_JNDI_PROVIDER_URL);
        runner.setProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, PROP_JNDI_CONNECTION_FACTORY_NAME);
    }

    private void configureLocalJMSConnectionFactory() {
        runner.setProperty(JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, PROP_JMS_CONNECTION_FACTORY_IMPL);
    }
}
