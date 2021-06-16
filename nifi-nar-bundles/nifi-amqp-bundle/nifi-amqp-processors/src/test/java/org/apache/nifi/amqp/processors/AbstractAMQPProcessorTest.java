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
package org.apache.nifi.amqp.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the AbstractAMQPProcessor class
 */
public class AbstractAMQPProcessorTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(ConsumeAMQP.class);

        testRunner.setProperty(ConsumeAMQP.QUEUE, "queue");
        testRunner.setProperty(AbstractAMQPProcessor.BROKERS, "localhost:5672");
    }

    @Test
    public void testValidUserPassword() {
        testRunner.setProperty(AbstractAMQPProcessor.USER, "user");
        testRunner.setProperty(AbstractAMQPProcessor.PASSWORD, "password");

        testRunner.assertValid();
    }

    @Test
    public void testNotValidUserMissing() {
        testRunner.setProperty(AbstractAMQPProcessor.PASSWORD, "password");

        testRunner.assertNotValid();
    }

    @Test
    public void testNotValidPasswordMissing() {
        testRunner.setProperty(AbstractAMQPProcessor.USER, "user");

        testRunner.assertNotValid();
    }

    @Test
    public void testNotValidBothUserPasswordAndClientCertAuth() throws Exception {
        testRunner.setProperty(AbstractAMQPProcessor.USER, "user");
        testRunner.setProperty(AbstractAMQPProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractAMQPProcessor.USE_CERT_AUTHENTICATION, "true");
        configureSSLContextService();

        testRunner.assertNotValid();
    }

    @Test
    public void testValidClientCertAuth() throws Exception {
        testRunner.setProperty(AbstractAMQPProcessor.USE_CERT_AUTHENTICATION, "true");
        configureSSLContextService();

        testRunner.assertValid();
    }

    @Test
    public void testNotValidClientCertAuthButNoSSLContextService() throws Exception {
        testRunner.setProperty(AbstractAMQPProcessor.USE_CERT_AUTHENTICATION, "true");

        testRunner.assertNotValid();
    }

    private void configureSSLContextService() throws InitializationException {
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        testRunner.addControllerService("ssl-context", sslService);
        testRunner.enableControllerService(sslService);

        testRunner.setProperty(AbstractAMQPProcessor.SSL_CONTEXT_SERVICE, "ssl-context");
    }
}
