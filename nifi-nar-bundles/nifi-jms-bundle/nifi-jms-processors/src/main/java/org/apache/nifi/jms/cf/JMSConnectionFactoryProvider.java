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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.controller.VerifiableControllerService;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSSecurityException;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides a factory service that creates and initializes
 * {@link ConnectionFactory} specific to the third party JMS system.
 * <p>
 * It accomplishes it by adjusting current classpath by adding to it the
 * additional resources (i.e., JMS client libraries) provided by the user via
 * {@link JMSConnectionFactoryProperties#JMS_CLIENT_LIBRARIES}, allowing it then to create an instance of the
 * target {@link ConnectionFactory} based on the provided
 * {@link JMSConnectionFactoryProperties#JMS_CONNECTION_FACTORY_IMPL} which can be than access via
 * {@link #getConnectionFactory()} method.
 */
@Tags({"jms", "messaging", "integration", "queue", "topic", "publish", "subscribe"})
@CapabilityDescription("Provides a generic service to create vendor specific javax.jms.ConnectionFactory implementations. "
        + "The Connection Factory can be served once this service is configured successfully.")
@DynamicProperty(name = "The name of a Connection Factory configuration property.", value = "The value of a given Connection Factory configuration property.",
        description = "The properties that are set following Java Beans convention where a property name is derived from the 'set*' method of the vendor "
                + "specific ConnectionFactory's implementation. For example, 'com.ibm.mq.jms.MQConnectionFactory.setChannel(String)' would imply 'channel' "
                + "property and 'com.ibm.mq.jms.MQConnectionFactory.setTransportType(int)' would imply 'transportType' property.",
                expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@SeeAlso(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS"})
public class JMSConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition, VerifiableControllerService {
    private static final String ESTABLISH_CONNECTION = "Establish Connection";
    private static final String VERIFY_JMS_INTERACTION = "Verify JMS Interaction";

    protected volatile JMSConnectionFactoryHandler delegate;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return JMSConnectionFactoryProperties.getPropertyDescriptors();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return JMSConnectionFactoryProperties.getDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        delegate = new JMSConnectionFactoryHandler(context, getLogger());
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
        final JMSConnectionFactoryHandler handler = new JMSConnectionFactoryHandler(context, verificationLogger);

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
                .explanation("Could not establish a Connection because doing so requires that a username and password be provided")
                .build());
        } catch (final Exception e) {
            logger.warn("Failed to establish a connection to the JMS Server in order to verify configuration", e);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(ESTABLISH_CONNECTION)
                .outcome(Outcome.FAILED)
                .explanation("Was not able to establish a connection to the JMS Server: " + e.toString())
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
                .explanation("Was not able to create a JMS Session: " + failure.toString())
                .build());
        }
    }
}
