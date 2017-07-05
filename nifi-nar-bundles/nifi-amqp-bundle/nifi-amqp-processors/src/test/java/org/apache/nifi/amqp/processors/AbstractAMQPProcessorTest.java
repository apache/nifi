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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


/**
 * Unit tests for the AbstractAMQPProcessor class
 */
public class AbstractAMQPProcessorTest {

    MockAbstractAMQPProcessor processor;
    private TestRunner testRunner;

    @Before
    public void setUp() throws Exception {
        processor = new MockAbstractAMQPProcessor();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test(expected = ProviderCreationException.class)
    public void testConnectToCassandraWithSSLBadClientAuth() throws Exception {
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        testRunner.addControllerService("ssl-context", sslService);
        testRunner.enableControllerService(sslService);
        testRunner.setProperty(AbstractAMQPProcessor.SSL_CONTEXT_SERVICE, "ssl-context");
        testRunner.setProperty(AbstractAMQPProcessor.USE_CERT_AUTHENTICATION, "false");
        testRunner.setProperty(AbstractAMQPProcessor.HOST, "test");
        testRunner.setProperty(AbstractAMQPProcessor.PORT, "9999");
        testRunner.setProperty(AbstractAMQPProcessor.USER, "test");
        testRunner.setProperty(AbstractAMQPProcessor.PASSWORD, "test");
        testRunner.assertValid(sslService);
        testRunner.setProperty(AbstractAMQPProcessor.CLIENT_AUTH, "BAD");
        processor.onTrigger(testRunner.getProcessContext(), testRunner.getProcessSessionFactory());
    }

    @Test(expected = ProviderCreationException.class)
    public void testInvalidSSLConfiguration() throws Exception {
        // it's invalid to have use_cert_auth enabled and not have the SSL Context Service configured
        testRunner.setProperty(AbstractAMQPProcessor.USE_CERT_AUTHENTICATION, "true");
        testRunner.setProperty(AbstractAMQPProcessor.HOST, "test");
        testRunner.setProperty(AbstractAMQPProcessor.PORT, "9999");
        testRunner.setProperty(AbstractAMQPProcessor.USER, "test");
        testRunner.setProperty(AbstractAMQPProcessor.PASSWORD, "test");
        processor.onTrigger(testRunner.getProcessContext(), testRunner.getProcessSessionFactory());
    }

    /**
     * Provides a stubbed processor instance for testing
     */
    public static class MockAbstractAMQPProcessor extends AbstractAMQPProcessor<AMQPConsumer> {
        @Override
        protected void rendezvousWithAmqp(ProcessContext context, ProcessSession session) throws ProcessException {
            // nothing to do
        }

        @Override
        protected AMQPConsumer finishBuildingTargetResource(ProcessContext context) {
            return null;
        }
    }
}
