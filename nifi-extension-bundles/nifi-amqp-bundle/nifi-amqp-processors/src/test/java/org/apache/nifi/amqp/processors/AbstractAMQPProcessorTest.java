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

import com.rabbitmq.client.Connection;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the AbstractAMQPProcessor class
 */
public class AbstractAMQPProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
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
        testRunner.setProperty(AbstractAMQPProcessor.CLIENT_CERTIFICATE_AUTHENTICATION_ENABLED, "true");
        configureSSLContextService();

        testRunner.assertNotValid();
    }

    @Test
    public void testValidClientCertAuth() throws Exception {
        testRunner.setProperty(AbstractAMQPProcessor.CLIENT_CERTIFICATE_AUTHENTICATION_ENABLED, "true");
        configureSSLContextService();

        testRunner.assertValid();
    }

    @Test
    public void testNotValidClientCertAuthButNoSSLContextService() throws Exception {
        testRunner.setProperty(AbstractAMQPProcessor.CLIENT_CERTIFICATE_AUTHENTICATION_ENABLED, "true");

        testRunner.assertNotValid();
    }

    @Test
    public void testExecutorShutdownWhenResourceCreationFails() throws Exception {
        final FailingWorkerCreationConsumeAMQP processor = new FailingWorkerCreationConsumeAMQP();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(ConsumeAMQP.QUEUE, "queue");
        runner.setProperty(AbstractAMQPProcessor.BROKERS, "localhost:5672");
        runner.setProperty(AbstractAMQPProcessor.USER, "user");
        runner.setProperty(AbstractAMQPProcessor.PASSWORD, "password");

        runner.run();

        assertTrue(processor.getExecutor().isShutdown());
        verify(processor.getConnection()).close();
    }

    private void configureSSLContextService() throws InitializationException {
        SSLContextProvider sslContextProvider = mock(SSLContextProvider.class);
        when(sslContextProvider.getIdentifier()).thenReturn("ssl-context");
        testRunner.addControllerService("ssl-context", sslContextProvider);
        testRunner.enableControllerService(sslContextProvider);

        testRunner.setProperty(AbstractAMQPProcessor.SSL_CONTEXT_SERVICE, "ssl-context");
    }

    private static class FailingWorkerCreationConsumeAMQP extends ConsumeAMQP {
        private final Connection connection = mock(Connection.class);
        private ExecutorService executor;

        private FailingWorkerCreationConsumeAMQP() {
            when(connection.isOpen()).thenReturn(true);
        }

        @Override
        protected Connection createConnection(final ProcessContext context, final ExecutorService executor) {
            this.executor = executor;
            return connection;
        }

        @Override
        protected AMQPConsumer createAMQPWorker(final ProcessContext context, final Connection connection) {
            throw new ProcessException("Worker creation failed");
        }

        private Connection getConnection() {
            return connection;
        }

        private ExecutorService getExecutor() {
            return executor;
        }
    }
}
