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
package org.apache.nifi.processors.kafka.pubsub;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for implementing {@link Processor}s to publish and consume
 * messages to/from Kafka
 *
 * @see PublishKafka
 * @see ConsumeKafka
 */
abstract class AbstractKafkaProcessor<T extends Closeable> extends AbstractSessionFactoryProcessor {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";

    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";


    static final AllowableValue SEC_PLAINTEXT = new AllowableValue("PLAINTEXT", "PLAINTEXT", "PLAINTEXT");
    static final AllowableValue SEC_SSL = new AllowableValue("SSL", "SSL", "SSL");
    static final AllowableValue SEC_SASL_PLAINTEXT = new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL_PLAINTEXT");
    static final AllowableValue SEC_SASL_SSL = new AllowableValue("SASL_SSL", "SASL_SSL", "SASL_SSL");

    static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
            .displayName("Kafka Brokers")
            .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(BROKER_REGEX)))
            .expressionLanguageSupported(true)
            .defaultValue("localhost:9092")
            .build();
    static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name(ProducerConfig.CLIENT_ID_CONFIG)
            .displayName("Client ID")
            .description("String value uniquely identifying this client application. Corresponds to Kafka's 'client.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Protocol used to communicate with brokers. Corresponds to Kafka's 'security.protocol' property.")
            .required(false)
            .expressionLanguageSupported(false)
            .allowableValues(SEC_PLAINTEXT, SEC_SSL, SEC_SASL_PLAINTEXT, SEC_SASL_SSL)
            .defaultValue(SEC_PLAINTEXT.getValue())
            .build();
    static final PropertyDescriptor KERBEROS_PRINCIPLE = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.service.name")
            .displayName("Kerberos Service Name")
            .description("The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or in Kafka's config. "
                    + "Corresponds to Kafka's 'security.protocol' property."
                    + "It is ignored unless one of the SASL options of the <Security Protocol> are selected.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("The name of the Kafka Topic")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service to use for communicating with Kafka.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final Builder MESSAGE_DEMARCATOR_BUILDER = new PropertyDescriptor.Builder()
            .name("message-demarcator")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true);

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are the are successfully sent to or received from Kafka are routed to this relationship")
            .build();

    static final List<PropertyDescriptor> SHARED_DESCRIPTORS = new ArrayList<>();

    static final Set<Relationship> SHARED_RELATIONSHIPS = new HashSet<>();

    private final AtomicInteger taskCounter = new AtomicInteger();

    private volatile boolean acceptTask = true;

    static {
        SHARED_DESCRIPTORS.add(BOOTSTRAP_SERVERS);
        SHARED_DESCRIPTORS.add(TOPIC);
        SHARED_DESCRIPTORS.add(CLIENT_ID);
        SHARED_DESCRIPTORS.add(SECURITY_PROTOCOL);
        SHARED_DESCRIPTORS.add(KERBEROS_PRINCIPLE);
        SHARED_DESCRIPTORS.add(SSL_CONTEXT_SERVICE);

        SHARED_RELATIONSHIPS.add(REL_SUCCESS);
    }

    /**
     * Instance of {@link KafkaPublisher} or {@link KafkaConsumer}
     */
    volatile T kafkaResource;

    /**
     * This thread-safe operation will delegate to
     * {@link #rendezvousWithKafka(ProcessContext, ProcessSession)} after first
     * checking and creating (if necessary) Kafka resource which could be either
     * {@link KafkaPublisher} or {@link KafkaConsumer}. It will also close and
     * destroy the underlying Kafka resource upon catching an {@link Exception}
     * raised by {@link #rendezvousWithKafka(ProcessContext, ProcessSession)}.
     * After Kafka resource is destroyed it will be re-created upon the next
     * invocation of this operation essentially providing a self healing mechanism
     * to deal with potentially corrupted resource.
     * <p>
     * Keep in mind that upon catching an exception the state of this processor
     * will be set to no longer accept any more tasks, until Kafka resource is reset.
     * This means that in a multi-threaded situation currently executing tasks will
     * be given a chance to complete while no new tasks will be accepted.
     */
    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.acceptTask) { // acts as a circuit breaker to allow existing tasks to wind down so 'kafkaResource' can be reset before new tasks are accepted.
            this.taskCounter.incrementAndGet();
            final ProcessSession session = sessionFactory.createSession();
            try {
                /*
                 * We can't be doing double null check here since as a pattern
                 * it only works for lazy init but not reset, which is what we
                 * are doing here. In fact the first null check is dangerous
                 * since 'kafkaResource' can become null right after its null
                 * check passed causing subsequent NPE.
                 */
                synchronized (this) {
                    if (this.kafkaResource == null) {
                        this.kafkaResource = this.buildKafkaResource(context, session);
                    }
                }

                /*
                 * The 'processed' boolean flag does not imply any failure or success. It simply states that:
                 * - ConsumeKafka - some messages were received form Kafka and 1_ FlowFile were generated
                 * - PublishKafka - some messages were sent to Kafka based on existence of the input FlowFile
                 */
                boolean processed = this.rendezvousWithKafka(context, session);
                session.commit();
                if (processed) {
                    this.postCommit(context);
                } else {
                    context.yield();
                }
            } catch (Throwable e) {
                this.acceptTask = false;
                session.rollback(true);
                this.getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, e });
            } finally {
                synchronized (this) {
                    if (this.taskCounter.decrementAndGet() == 0 && !this.acceptTask) {
                        this.close();
                        this.acceptTask = true;
                    }
                }
            }
        } else {
            this.logger.debug("Task was not accepted due to the processor being in 'reset' state. It will be re-submitted upon completion of the reset.");
            this.getLogger().debug("Task was not accepted due to the processor being in 'reset' state. It will be re-submitted upon completion of the reset.");
            context.yield();
        }
    }

    /**
     * Will call {@link Closeable#close()} on the target resource after which
     * the target resource will be set to null. Should only be called when there
     * are no more threads being executed on this processor or when it has been
     * verified that only a single thread remains.
     *
     * @see KafkaPublisher
     * @see KafkaConsumer
     */
    @OnStopped
    public void close() {
        try {
            if (this.kafkaResource != null) {
                try {
                    this.kafkaResource.close();
                } catch (Exception e) {
                    this.getLogger().warn("Failed while closing " + this.kafkaResource, e);
                }
            }
        } finally {
            this.kafkaResource = null;
        }
    }

    /**
     *
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     * This operation is called from
     * {@link #onTrigger(ProcessContext, ProcessSessionFactory)} method and
     * contains main processing logic for this Processor.
     */
    protected abstract boolean rendezvousWithKafka(ProcessContext context, ProcessSession session);

    /**
     * Builds target resource for interacting with Kafka. The target resource
     * could be one of {@link KafkaPublisher} or {@link KafkaConsumer}
     */
    protected abstract T buildKafkaResource(ProcessContext context, ProcessSession session);

    /**
     * This operation will be executed after {@link ProcessSession#commit()} has
     * been called.
     */
    protected void postCommit(ProcessContext context) {
        // no op
    }

    /**
     *
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();

        String securityProtocol = validationContext.getProperty(SECURITY_PROTOCOL).getValue();

        /*
         * validates that if one of SASL (Kerberos) option is selected for
         * security protocol, then Kerberos principal is provided as well
         */
        if (SEC_SASL_PLAINTEXT.getValue().equals(securityProtocol) || SEC_SASL_SSL.getValue().equals(securityProtocol)){
            String kerberosPrincipal = validationContext.getProperty(KERBEROS_PRINCIPLE).getValue();
            if (kerberosPrincipal == null || kerberosPrincipal.trim().length() == 0){
                results.add(new ValidationResult.Builder().subject(KERBEROS_PRINCIPLE.getDisplayName()).valid(false)
                        .explanation("The <" + KERBEROS_PRINCIPLE.getDisplayName() + "> property must be set when <"
                                + SECURITY_PROTOCOL.getDisplayName() + "> is configured as '"
                                + SEC_SASL_PLAINTEXT.getValue() + "' or '" + SEC_SASL_SSL.getValue() + "'.")
                        .build());
            }
        }

        String keySerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).build())
                .getValue();
        if (keySerializer != null && !ByteArraySerializer.class.getName().equals(keySerializer)) {
            results.add(new ValidationResult.Builder().subject(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                    .explanation("Key Serializer must be " + ByteArraySerializer.class.getName() + "' was '" + keySerializer + "'").build());
        }
        String valueSerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).build())
                .getValue();
        if (valueSerializer != null && !ByteArraySerializer.class.getName().equals(valueSerializer)) {
            results.add(new ValidationResult.Builder().subject(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                    .explanation("Value Serializer must be " + ByteArraySerializer.class.getName() + "' was '" + valueSerializer + "'").build());
        }
        String keyDeSerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).build())
                .getValue();
        if (keyDeSerializer != null && !ByteArrayDeserializer.class.getName().equals(keyDeSerializer)) {
            results.add(new ValidationResult.Builder().subject(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Key De-Serializer must be '" + ByteArrayDeserializer.class.getName() + "' was '" + keyDeSerializer + "'").build());
        }
        String valueDeSerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).build())
                .getValue();
        if (valueDeSerializer != null && !ByteArrayDeserializer.class.getName().equals(valueDeSerializer)) {
            results.add(new ValidationResult.Builder().subject(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Value De-Serializer must be " + ByteArrayDeserializer.class.getName() + "' was '" + valueDeSerializer + "'").build());
        }

        return results;
    }

    /**
     * Builds transit URI for provenance event. The transit URI will be in the
     * form of &lt;security.protocol&gt;://&lt;bootstrap.servers&gt;/topic
     */
    String buildTransitURI(String securityProtocol, String brokers, String topic) {
        StringBuilder builder = new StringBuilder();
        builder.append(securityProtocol);
        builder.append("://");
        builder.append(brokers);
        builder.append("/");
        builder.append(topic);
        return builder.toString();
    }

    /**
     * Builds Kafka {@link Properties}
     */
    Properties buildKafkaProperties(ProcessContext context) {
        Properties properties = new Properties();
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            if (propertyDescriptor.equals(SSL_CONTEXT_SERVICE)) {
                // Translate SSLContext Service configuration into Kafka properties
                final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                buildSSLKafkaProperties(sslContextService, properties);
                continue;
            }

            String pName = propertyDescriptor.getName();
            String pValue = propertyDescriptor.isExpressionLanguageSupported()
                    ? context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue()
                    : context.getProperty(propertyDescriptor).getValue();
            if (pValue != null) {
                if (pName.endsWith(".ms")) { // kafka standard time notation
                    pValue = String.valueOf(FormatUtils.getTimeDuration(pValue.trim(), TimeUnit.MILLISECONDS));
                }
                properties.setProperty(pName, pValue);
            }
        }
        return properties;
    }

    private void buildSSLKafkaProperties(final SSLContextService sslContextService, final Properties properties) {
        if (sslContextService == null) {
            return;
        }

        if (sslContextService.isKeyStoreConfigured()) {
            properties.setProperty("ssl.keystore.location", sslContextService.getKeyStoreFile());
            properties.setProperty("ssl.keystore.password", sslContextService.getKeyStorePassword());
            final String keyPass = sslContextService.getKeyPassword() == null ? sslContextService.getKeyStorePassword() : sslContextService.getKeyPassword();
            properties.setProperty("ssl.key.password", keyPass);
            properties.setProperty("ssl.keystore.type", sslContextService.getKeyStoreType());
        }

        if (sslContextService.isTrustStoreConfigured()) {
            properties.setProperty("ssl.truststore.location", sslContextService.getTrustStoreFile());
            properties.setProperty("ssl.truststore.password", sslContextService.getTrustStorePassword());
            properties.setProperty("ssl.truststore.type", sslContextService.getTrustStoreType());
        }
    }
}
