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

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base JMS controller service implementation that provides verification logic.
 */
public abstract class AbstractJMSConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition, VerifiableControllerService {
    private static final String ESTABLISH_CONNECTION = "Establish Connection";
    private static final String VERIFY_JMS_INTERACTION = "Verify JMS Interaction";

    protected volatile JMSConnectionFactoryHandlerDefinition delegate;

    protected abstract JMSConnectionFactoryHandlerDefinition createConnectionFactoryHandler(ConfigurationContext context, ComponentLog logger);

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        delegate = createConnectionFactoryHandler(context, getLogger());
    }

    @OnDisabled
    public void onDisabled() {
        delegate = null;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return delegate.getConnectionFactory();
    }

    @Override
    public void resetConnectionFactory(ConnectionFactory cachedFactory) {
        delegate.resetConnectionFactory(cachedFactory);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final IJMSConnectionFactoryProvider handler = createConnectionFactoryHandler(context, verificationLogger);

        final AtomicReference<Exception> failureReason = new AtomicReference<>();
        final ExceptionListener listener = failureReason::set;

        final Connection connection = createConnection(handler.getConnectionFactory(), results, listener, verificationLogger);
        if (connection != null) {
            try {
                createSession(connection, results, failureReason.get(), verificationLogger);
            } finally {
                try {
                    connection.close();
                } catch (final Exception ignored) {
                }
            }
        }

        return results;
    }

    private Connection createConnection(final ConnectionFactory connectionFactory, final List<ConfigVerificationResult> results, final ExceptionListener exceptionListener, final ComponentLog logger) {
        try {
            final Connection connection = connectionFactory.createConnection();
            connection.setExceptionListener(exceptionListener);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(ESTABLISH_CONNECTION)
                .outcome(Outcome.SUCCESSFUL)
                .explanation("Successfully established a JMS Connection")
                .build());

            return connection;
        } catch (final JMSSecurityException se) {
            // If we encounter a JMS Security Exception, the documentation states that it is because of an invalid username or password.
            // There is no username or password configured for the Controller Service itself, however. Those are configured in processors, etc.
            // As a result, if this is encountered, we will skip verification.
            logger.debug("Failed to establish a connection to the JMS Server in order to verify configuration because encountered JMS Security Exception", se);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(ESTABLISH_CONNECTION)
                .outcome(Outcome.SKIPPED)
                .explanation("Could not establish a Connection because doing so requires a valid username and password")
                .build());
        } catch (final Exception e) {
            logger.warn("Failed to establish a connection to the JMS Server in order to verify configuration", e);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(ESTABLISH_CONNECTION)
                .outcome(Outcome.FAILED)
                .explanation("Was not able to establish a connection to the JMS Server: " + e)
                .build());
        }

        return null;
    }

    private void createSession(final Connection connection, final List<ConfigVerificationResult> results, final Exception capturedException, final ComponentLog logger) {
        try {
            final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            session.close();

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(VERIFY_JMS_INTERACTION)
                .outcome(Outcome.SUCCESSFUL)
                .explanation("Established a JMS Session with server and successfully terminated it")
                .build());
        } catch (final Exception e) {
            final Exception failure;
            if (capturedException == null) {
                failure = e;
            } else {
                failure = capturedException;
                failure.addSuppressed(e);
            }

            logger.warn("Failed to create a JMS Session in order to verify configuration", failure);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(VERIFY_JMS_INTERACTION)
                .outcome(Outcome.FAILED)
                .explanation("Was not able to create a JMS Session: " + failure)
                .build());
        }
    }
}
