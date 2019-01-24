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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base JMS processor to support implementation of JMS producers and consumers.
 *
 * @param <T> the type of {@link JMSWorker} which could be {@link JMSPublisher} or {@link JMSConsumer}
 * @see PublishJMS
 * @see ConsumeJMS
 * @see JMSConnectionFactoryProviderDefinition
 */
abstract class AbstractJMSProcessor<T extends JMSWorker> extends AbstractProcessor {

    static final String QUEUE = "QUEUE";
    static final String TOPIC = "TOPIC";
    static final String TEXT_MESSAGE = "text";
    static final String BYTES_MESSAGE = "bytes";

    static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User Name")
            .description("User Name used for authentication and authorization.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor SESSION_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Session Cache size")
            .description("This property is deprecated and no longer has any effect on the Processor. It will be removed in a later version.")
            .required(false)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor MESSAGE_BODY = new PropertyDescriptor.Builder()
            .name("message-body-type")
            .displayName("Message Body Type")
            .description("The type of JMS message body to construct.")
            .required(true)
            .defaultValue(BYTES_MESSAGE)
            .allowableValues(BYTES_MESSAGE, TEXT_MESSAGE)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
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
            .description("The Controller Service that is used to obtain ConnectionFactory")
            .required(true)
            .identifiesControllerService(JMSConnectionFactoryProviderDefinition.class)
            .build();

    static final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
    private volatile BlockingQueue<T> workerPool;
    private final AtomicInteger clientIdCounter = new AtomicInteger(1);

    static {
        propertyDescriptors.add(CF_SERVICE);
        propertyDescriptors.add(DESTINATION);
        propertyDescriptors.add(DESTINATION_TYPE);
        propertyDescriptors.add(USER);
        propertyDescriptors.add(PASSWORD);
        propertyDescriptors.add(CLIENT_ID);
        propertyDescriptors.add(SESSION_CACHE_SIZE);
        propertyDescriptors.add(MESSAGE_BODY);
        propertyDescriptors.add(CHARSET);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        T worker = workerPool.poll();
        if (worker == null) {
            worker = buildTargetResource(context);
        }

        try {
            rendezvousWithJms(context, session, worker);
        } finally {
            workerPool.offer(worker);
        }
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
        final JMSConnectionFactoryProviderDefinition cfProvider = context.getProperty(CF_SERVICE).asControllerService(JMSConnectionFactoryProviderDefinition.class);
        final ConnectionFactory connectionFactory = cfProvider.getConnectionFactory();

        final UserCredentialsConnectionFactoryAdapter cfCredentialsAdapter = new UserCredentialsConnectionFactoryAdapter();
        cfCredentialsAdapter.setTargetConnectionFactory(connectionFactory);
        cfCredentialsAdapter.setUsername(context.getProperty(USER).evaluateAttributeExpressions().getValue());
        cfCredentialsAdapter.setPassword(context.getProperty(PASSWORD).getValue());

        final CachingConnectionFactory cachingFactory = new CachingConnectionFactory(cfCredentialsAdapter);

        String clientId = context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue();
        if (clientId != null) {
            clientId = clientId + "-" + clientIdCounter.getAndIncrement();
            cachingFactory.setClientId(clientId);
        }

        JmsTemplate jmsTemplate = new JmsTemplate();
        jmsTemplate.setConnectionFactory(cachingFactory);
        jmsTemplate.setPubSubDomain(TOPIC.equals(context.getProperty(DESTINATION_TYPE).getValue()));

        return finishBuildingJmsWorker(cachingFactory, jmsTemplate, context);
    }
}
