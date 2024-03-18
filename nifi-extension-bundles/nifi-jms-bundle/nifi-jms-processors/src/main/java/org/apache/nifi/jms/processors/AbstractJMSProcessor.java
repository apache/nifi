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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.jms.cf.IJMSConnectionFactoryProvider;
import org.apache.nifi.jms.cf.JMSConnectionFactoryHandler;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProperties;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.jms.cf.JndiJmsConnectionFactoryHandler;
import org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;
import org.springframework.jms.core.JmsTemplate;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Base JMS processor to support implementation of JMS producers and consumers.
 *
 * @param <T> the type of {@link JMSWorker} which could be {@link JMSPublisher} or {@link JMSConsumer}
 * @see PublishJMS
 * @see ConsumeJMS
 * @see JMSConnectionFactoryProviderDefinition
 */
public abstract class AbstractJMSProcessor<T extends JMSWorker> extends AbstractProcessor {

    static final String QUEUE = "QUEUE";
    static final String TOPIC = "TOPIC";
    static final String TEXT_MESSAGE = "text";
    static final String BYTES_MESSAGE = "bytes";

    static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User Name")
            .description("User Name used for authentication and authorization.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();
    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password used for authentication and authorization.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination Name")
            .description("The name of the JMS Destination. Usually provided by the administrator (e.g., 'topic://myTopic' or 'myTopic').")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor DESTINATION_TYPE = new PropertyDescriptor.Builder()
            .name("Destination Type")
            .description("The type of the JMS Destination. Could be one of 'QUEUE' or 'TOPIC'. Usually provided by the administrator. Defaults to 'QUEUE'")
            .required(true)
            .allowableValues(QUEUE, TOPIC)
            .defaultValue(QUEUE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Connection Client ID")
            .description("The client id to be set on the connection, if set. For durable non shared consumer this is mandatory, " +
                         "for all others it is optional, typically with shared consumers it is undesirable to be set. " +
                         "Please see JMS spec for further details")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("character-set")
            .displayName("Character Set")
            .description("The name of the character set to use to construct or interpret TextMessages")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(Charset.defaultCharset().name())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor CF_SERVICE = new PropertyDescriptor.Builder()
            .name("Connection Factory Service")
            .description("The Controller Service that is used to obtain Connection Factory. Alternatively, the 'JNDI *' or the 'JMS *' properties " +
                    "can also be be used to configure the Connection Factory.")
            .required(false)
            .identifiesControllerService(JMSConnectionFactoryProviderDefinition.class)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Batch Size")
            .description("The maximum number of messages to publish or consume in each invocation of the processor.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.createLongValidator(1, 10_000, true))
            .build();

    static final List<PropertyDescriptor> JNDI_JMS_CF_PROPERTIES = Collections.unmodifiableList(
            JndiJmsConnectionFactoryProperties.getPropertyDescriptors().stream()
                    .map(pd -> new PropertyDescriptor.Builder()
                            .fromPropertyDescriptor(pd)
                            .required(false)
                            .build())
                    .collect(Collectors.toList())
    );

    static final List<PropertyDescriptor> JMS_CF_PROPERTIES = Collections.unmodifiableList(
            JMSConnectionFactoryProperties.getPropertyDescriptors().stream()
                    .map(pd -> new PropertyDescriptor.Builder()
                            .fromPropertyDescriptor(pd)
                            .required(false)
                            .build())
                    .collect(Collectors.toList())
    );

    static final PropertyDescriptor BASE_RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    static final PropertyDescriptor BASE_RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .dependsOn(BASE_RECORD_READER)
            .required(true)
            .build();

    private volatile IJMSConnectionFactoryProvider connectionFactoryProvider;
    private volatile BlockingQueue<T> workerPool;
    private final AtomicInteger clientIdCounter = new AtomicInteger(1);

    protected static String getClientId(ProcessContext context) {
        return context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Additional configuration property for the Connection Factory")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.removeProperty("Session Cache size");
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return new ConnectionFactoryConfigValidator(validationContext).validateConnectionFactoryConfig();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        T worker = workerPool.poll();
        if (worker == null) {
            try {
                worker = buildTargetResource(context);
            } catch (Exception e) {
                getLogger().error("Failed to initialize JMS Connection Factory", e);
                context.yield();
                throw e;
            }
        }

        try {
            rendezvousWithJms(context, session, worker);
        } finally {
            //in case of exception during worker's connection (consumer or publisher),
            //an appropriate service is responsible to invalidate the worker.
            //if worker is not valid anymore, don't put it back into a pool, try to rebuild it first, or discard.
            //this will be helpful in a situation, when JNDI has changed, or JMS server is not available
            //and reconnection is required.
            if (worker == null || !worker.isValid()) {
                getLogger().debug("Worker is invalid. Will try re-create... ");
                try {
                    if (worker != null) {
                        worker.shutdown();
                    }
                    // Safe to cast. Method #buildTargetResource(ProcessContext context) sets only CachingConnectionFactory
                    CachingConnectionFactory currentCF = (CachingConnectionFactory) worker.jmsTemplate.getConnectionFactory();
                    connectionFactoryProvider.resetConnectionFactory(currentCF.getTargetConnectionFactory());
                    worker = buildTargetResource(context);
                } catch (Exception e) {
                    getLogger().error("Failed to rebuild:  {}", connectionFactoryProvider);
                    worker = null;
                }
            }
            if (worker != null) {
                worker.jmsTemplate.setExplicitQosEnabled(false);
                worker.jmsTemplate.setDeliveryMode(Message.DEFAULT_DELIVERY_MODE);
                worker.jmsTemplate.setTimeToLive(Message.DEFAULT_TIME_TO_LIVE);
                worker.jmsTemplate.setPriority(Message.DEFAULT_PRIORITY);
                workerPool.offer(worker);
            }
        }
    }

    @OnScheduled
    public void setupConnectionFactoryProvider(final ProcessContext context) {
        if (context.getProperty(CF_SERVICE).isSet()) {
            connectionFactoryProvider = context.getProperty(CF_SERVICE).asControllerService(JMSConnectionFactoryProviderDefinition.class);
        } else if (context.getProperty(JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME).isSet()) {
            connectionFactoryProvider = new JndiJmsConnectionFactoryHandler(context, getLogger());
        } else if (context.getProperty(JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL).isSet()) {
            connectionFactoryProvider = new JMSConnectionFactoryHandler(context, getLogger());
        } else {
            throw new ProcessException("No Connection Factory configured.");
        }
    }

    @OnUnscheduled
    public void shutdownConnectionFactoryProvider(final ProcessContext context) {
        connectionFactoryProvider = null;
    }

    @OnScheduled
    public void setupWorkerPool(final ProcessContext context) {
        workerPool = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());
    }


    @OnStopped
    public void close() {
        T worker;
        while ((worker = workerPool.poll()) != null) {
            worker.shutdown();
        }
    }

    /**
     * Delegate method to supplement
     * {@link #onTrigger(ProcessContext, ProcessSession)} operation. It is
     * implemented by sub-classes to perform {@link Processor} specific
     * functionality.
     */
    protected abstract void rendezvousWithJms(ProcessContext context, ProcessSession session, T jmsWorker) throws ProcessException;

    /**
     * Finishes building one of the {@link JMSWorker} subclasses T.
     *
     * @see JMSPublisher
     * @see JMSConsumer
     */
    protected abstract T finishBuildingJmsWorker(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ProcessContext processContext);

    /**
     * This method essentially performs initialization of this Processor by
     * obtaining an instance of the {@link ConnectionFactory} from the
     * {@link JMSConnectionFactoryProvider} (ControllerService) and performing a
     * series of {@link ConnectionFactory} adaptations which eventually results
     * in an instance of the {@link CachingConnectionFactory} used to construct
     * {@link JmsTemplate} used by this Processor.
     */
    private T buildTargetResource(ProcessContext context) {
        final ConnectionFactory connectionFactory = connectionFactoryProvider.getConnectionFactory();

        final UserCredentialsConnectionFactoryAdapter cfCredentialsAdapter = new UserCredentialsConnectionFactoryAdapter();
        cfCredentialsAdapter.setTargetConnectionFactory(connectionFactory);
        cfCredentialsAdapter.setUsername(context.getProperty(USER).evaluateAttributeExpressions().getValue());
        cfCredentialsAdapter.setPassword(context.getProperty(PASSWORD).getValue());

        final CachingConnectionFactory cachingFactory = new CachingConnectionFactory(cfCredentialsAdapter);
        setClientId(context, cachingFactory);

        JmsTemplate jmsTemplate = new JmsTemplate();
        jmsTemplate.setConnectionFactory(cachingFactory);
        jmsTemplate.setPubSubDomain(TOPIC.equals(context.getProperty(DESTINATION_TYPE).getValue()));

        return finishBuildingJmsWorker(cachingFactory, jmsTemplate, context);
    }

    /**
     * Set clientId for JMS connections when <tt>clientId</tt> is not null.
     * It is overridden by {@code}ConsumeJMS{@code} when durable subscriptions
     * is configured on the processor.
     * @param context context.
     * @param connectionFactory the connection factory.
     * @since NIFI-6915
     */
    protected void setClientId(ProcessContext context, final SingleConnectionFactory connectionFactory) {
        String clientId = getClientId(context);
        if (clientId != null) {
            clientId = clientId + "-" + clientIdCounter.getAndIncrement();
            connectionFactory.setClientId(clientId);
        }
    }

    static class ConnectionFactoryConfigValidator {

        private final ValidationContext validationContext;

        private final PropertyValue connectionFactoryServiceProperty;
        private final PropertyValue jndiInitialContextFactoryProperty;
        private final PropertyValue jmsConnectionFactoryImplProperty;

        ConnectionFactoryConfigValidator(ValidationContext validationContext) {
            this.validationContext = validationContext;

            connectionFactoryServiceProperty = validationContext.getProperty(CF_SERVICE);
            jndiInitialContextFactoryProperty = validationContext.getProperty(JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY);
            jmsConnectionFactoryImplProperty = validationContext.getProperty(JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL);
        }

        List<ValidationResult> validateConnectionFactoryConfig() {
            List<ValidationResult> results = new ArrayList<>();

            if (!(connectionFactoryServiceProperty.isSet() || jndiInitialContextFactoryProperty.isSet() || jmsConnectionFactoryImplProperty.isSet())) {
                results.add(new ValidationResult.Builder()
                        .subject("Connection Factory config")
                        .valid(false)
                        .explanation(String.format("either '%s', '%s' or '%s' must be specified.", CF_SERVICE.getDisplayName(),
                                JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY.getDisplayName(), JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL.getDisplayName()))
                        .build());
            } else if (connectionFactoryServiceProperty.isSet()) {
                if (hasLocalJndiJmsConnectionFactoryConfig()) {
                    results.add(new ValidationResult.Builder()
                            .subject("Connection Factory config")
                            .valid(false)
                            .explanation(String.format("cannot set both '%s' and 'JNDI *' properties.", CF_SERVICE.getDisplayName()))
                            .build());
                }
                if (hasLocalJMSConnectionFactoryConfig()) {
                    results.add(new ValidationResult.Builder()
                            .subject("Connection Factory config")
                            .valid(false)
                            .explanation(String.format("cannot set both '%s' and 'JMS *' properties.", CF_SERVICE.getDisplayName()))
                            .build());
                }
            } else if (hasLocalJndiJmsConnectionFactoryConfig() && hasLocalJMSConnectionFactoryConfig()) {
                results.add(new ValidationResult.Builder()
                        .subject("Connection Factory config")
                        .valid(false)
                        .explanation("cannot set both 'JNDI *' and 'JMS *' properties.")
                        .build());
            } else if (jndiInitialContextFactoryProperty.isSet()) {
                validateLocalConnectionFactoryConfig(JndiJmsConnectionFactoryProperties.getPropertyDescriptors(), JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY, results);
            } else if (jmsConnectionFactoryImplProperty.isSet()) {
                validateLocalConnectionFactoryConfig(JMSConnectionFactoryProperties.getPropertyDescriptors(), JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL, results);
            }

            return results;
        }

        private boolean hasLocalJndiJmsConnectionFactoryConfig() {
            return hasLocalConnectionFactoryConfig(JndiJmsConnectionFactoryProperties.getPropertyDescriptors());
        }

        private boolean hasLocalJMSConnectionFactoryConfig() {
            return hasLocalConnectionFactoryConfig(JMSConnectionFactoryProperties.getPropertyDescriptors());
        }

        private boolean hasLocalConnectionFactoryConfig(List<PropertyDescriptor> localConnectionFactoryProperties) {
            for (PropertyDescriptor propertyDescriptor : localConnectionFactoryProperties) {
                PropertyValue propertyValue = validationContext.getProperty(propertyDescriptor);
                if (propertyValue.isSet()) {
                    return true;
                }
            }
            return false;
        }

        private void validateLocalConnectionFactoryConfig(List<PropertyDescriptor> localConnectionFactoryProperties, PropertyDescriptor indicatorProperty, List<ValidationResult> results) {
            for (PropertyDescriptor propertyDescriptor : localConnectionFactoryProperties) {
                if (propertyDescriptor.isRequired()) {
                    PropertyValue propertyValue = validationContext.getProperty(propertyDescriptor);
                    if (!propertyValue.isSet()) {
                        results.add(new ValidationResult.Builder()
                                .subject("Connection Factory config")
                                .valid(false)
                                .explanation(String.format("'%s' must be specified when '%s' has been configured.", propertyDescriptor.getDisplayName(), indicatorProperty.getDisplayName()))
                                .build());
                    }
                }
            }
        }
    }
}
