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

import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
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

/**
 * Base JMS processor to support implementation of JMS producers and consumers.
 *
 * @param <T>
 *            the type of {@link JMSWorker} which could be {@link JMSPublisher}
 *            or {@link JMSConsumer}
 * @see PublishJMS
 * @see ConsumeJMS
 * @see JMSConnectionFactoryProviderDefinition
 */
abstract class AbstractJMSProcessor<T extends JMSWorker> extends AbstractProcessor {

    static final String QUEUE = "QUEUE";

    static final String TOPIC = "TOPIC";

    static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User Name")
            .description("User Name used for authentication and authorization.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor DESTINATION_TYPE = new PropertyDescriptor.Builder()
            .name("Destination Type")
            .description("The type of the JMS Destination. Could be one of 'QUEUE' or 'TOPIC'. Usually provided by the administrator. Defaults to 'TOPIC")
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
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor SESSION_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Session Cache size")
            .description("The maximum limit for the number of cached Sessions.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    // ConnectionFactoryProvider ControllerService
    static final PropertyDescriptor CF_SERVICE = new PropertyDescriptor.Builder()
            .name("Connection Factory Service")
            .description("The Controller Service that is used to obtain ConnectionFactory")
            .required(true)
            .identifiesControllerService(JMSConnectionFactoryProviderDefinition.class)
            .build();

    static final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    static {
        propertyDescriptors.add(CF_SERVICE);
        propertyDescriptors.add(DESTINATION);
        propertyDescriptors.add(DESTINATION_TYPE);
        propertyDescriptors.add(USER);
        propertyDescriptors.add(PASSWORD);
        propertyDescriptors.add(CLIENT_ID);
        propertyDescriptors.add(SESSION_CACHE_SIZE);
    }

    protected volatile T targetResource;

    private volatile CachingConnectionFactory cachingConnectionFactory;

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     * Builds target resource ({@link JMSPublisher} or {@link JMSConsumer}) upon
     * first invocation while delegating to the sub-classes ( {@link PublishJMS}
     * or {@link ConsumeJMS}) via
     * {@link #rendezvousWithJms(ProcessContext, ProcessSession)} method.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        synchronized (this) {
            this.buildTargetResource(context);
        }
        this.rendezvousWithJms(context, session);
    }

    /**
     * Will destroy the instance of {@link CachingConnectionFactory} and sets
     * 'targetResource' to null;
     */
    @OnStopped
    public void close() {
        if (this.cachingConnectionFactory != null) {
            this.cachingConnectionFactory.destroy();
        }
        this.targetResource = null;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " - " + this.targetResource;
    }

    /**
     * Delegate method to supplement
     * {@link #onTrigger(ProcessContext, ProcessSession)} operation. It is
     * implemented by sub-classes to perform {@link Processor} specific
     * functionality.
     *
     * @param context
     *            instance of {@link ProcessContext}
     * @param session
     *            instance of {@link ProcessSession}
     */
    protected abstract void rendezvousWithJms(ProcessContext context, ProcessSession session) throws ProcessException;

    /**
     * Finishes building one of the {@link JMSWorker} subclasses T.
     *
     * @param jmsTemplate instance of {@link JmsTemplate}
     *
     * @see JMSPublisher
     * @see JMSConsumer
     */
    protected abstract T finishBuildingTargetResource(JmsTemplate jmsTemplate, ProcessContext processContext);

    /**
     * This method essentially performs initialization of this Processor by
     * obtaining an instance of the {@link ConnectionFactory} from the
     * {@link JMSConnectionFactoryProvider} (ControllerService) and performing a
     * series of {@link ConnectionFactory} adaptations which eventually results
     * in an instance of the {@link CachingConnectionFactory} used to construct
     * {@link JmsTemplate} used by this Processor.
     */
    private void buildTargetResource(ProcessContext context) {
        if (this.targetResource == null) {
            JMSConnectionFactoryProviderDefinition cfProvider = context.getProperty(CF_SERVICE).asControllerService(JMSConnectionFactoryProviderDefinition.class);
            ConnectionFactory connectionFactory = cfProvider.getConnectionFactory();

            UserCredentialsConnectionFactoryAdapter cfCredentialsAdapter = new UserCredentialsConnectionFactoryAdapter();
            cfCredentialsAdapter.setTargetConnectionFactory(connectionFactory);
            cfCredentialsAdapter.setUsername(context.getProperty(USER).getValue());
            cfCredentialsAdapter.setPassword(context.getProperty(PASSWORD).getValue());

            this.cachingConnectionFactory = new CachingConnectionFactory(cfCredentialsAdapter);
            this.cachingConnectionFactory.setSessionCacheSize(Integer.parseInt(context.getProperty(SESSION_CACHE_SIZE).getValue()));
            String clientId = context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue();
            if (clientId != null) {
                this.cachingConnectionFactory.setClientId(clientId);
            }
            JmsTemplate jmsTemplate = new JmsTemplate();
            jmsTemplate.setConnectionFactory(this.cachingConnectionFactory);
            jmsTemplate.setPubSubDomain(TOPIC.equals(context.getProperty(DESTINATION_TYPE).getValue()));

            // set of properties that may be good candidates for exposure via configuration
            jmsTemplate.setReceiveTimeout(1000);

            this.targetResource = this.finishBuildingTargetResource(jmsTemplate, context);
        }
    }
}
