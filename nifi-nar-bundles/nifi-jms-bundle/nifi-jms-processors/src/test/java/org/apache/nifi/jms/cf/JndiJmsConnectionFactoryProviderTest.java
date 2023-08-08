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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JndiJmsConnectionFactoryProviderTest {

    private static final String SERVICE_ID = JndiJmsConnectionFactoryProvider.class.getSimpleName();

    private static final String CONTEXT_FACTORY = "ContextFactory";

    private static final String FACTORY_NAME = "ConnectionFactory";

    private static final String TCP_PROVIDER_URL = "tcp://127.0.0.1";

    private static final String LDAP_PROVIDER_URL = "ldap://127.0.0.1";

    private static final String LDAP_PROVIDER_URL_SPACED = String.format(" %s", LDAP_PROVIDER_URL);

    private static final String LDAP_PROVIDER_URL_EXPRESSION = "ldap:${separator}//127.0.0.1";

    private static final String HOST_PORT_URL = "127.0.0.1:1024";

    private static final String LDAP_ALLOWED_URL_SCHEMES = "ldap";

    private TestRunner runner;

    private JndiJmsConnectionFactoryProvider provider;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        provider = new JndiJmsConnectionFactoryProvider();
        runner.addControllerService(SERVICE_ID, provider);
    }

    @Test
    void testPropertiesValid() {
        setFactoryProperties();

        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, TCP_PROVIDER_URL);

        runner.assertValid(provider);
    }

    @Test
    void testPropertiesInvalidUrlNotConfigured() {
        setFactoryProperties();

        runner.assertNotValid(provider);
    }

    @Test
    void testPropertiesInvalidUrlScheme() {
        setFactoryProperties();

        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, LDAP_PROVIDER_URL);

        runner.assertNotValid(provider);
    }

    @Test
    void testPropertiesInvalidUrlSchemeSpaced() {
        setFactoryProperties();

        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, LDAP_PROVIDER_URL_SPACED);

        runner.assertNotValid(provider);
    }

    @Test
    void testPropertiesInvalidUrlSchemeExpression() {
        setFactoryProperties();

        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, LDAP_PROVIDER_URL_EXPRESSION);

        runner.assertNotValid(provider);
    }

    @Test
    void testPropertiesHostPortUrl() {
        setFactoryProperties();

        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL, HOST_PORT_URL);

        runner.assertValid(provider);
    }

    @Test
    void testUrlSchemeValidSystemProperty() {
        try {
            System.setProperty(JndiJmsConnectionFactoryProperties.URL_SCHEMES_ALLOWED_PROPERTY, LDAP_ALLOWED_URL_SCHEMES);

            final MockProcessContext processContext = new MockProcessContext(new NoOpProcessor());
            final MockValidationContext validationContext = new MockValidationContext(processContext);

            final JndiJmsConnectionFactoryProperties.JndiJmsProviderUrlValidator validator = new JndiJmsConnectionFactoryProperties.JndiJmsProviderUrlValidator();
            final ValidationResult result = validator.validate(JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL.getDisplayName(), LDAP_PROVIDER_URL, validationContext);

            assertNotNull(result);
            assertTrue(result.isValid());
        } finally {
            System.clearProperty(JndiJmsConnectionFactoryProperties.URL_SCHEMES_ALLOWED_PROPERTY);
        }
    }

    private void setFactoryProperties() {
        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, CONTEXT_FACTORY);
        runner.setProperty(provider, JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME, FACTORY_NAME);
    }
}
