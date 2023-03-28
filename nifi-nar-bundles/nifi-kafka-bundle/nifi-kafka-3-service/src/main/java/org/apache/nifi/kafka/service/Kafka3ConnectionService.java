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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.ServiceConfiguration;
import org.apache.nifi.kafka.service.api.consumer.ConsumerConfiguration;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.consumer.Kafka3ConsumerService;
import org.apache.nifi.kafka.service.producer.Kafka3ProducerService;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.provider.KafkaPropertyProvider;
import org.apache.nifi.kafka.shared.property.provider.StandardKafkaPropertyProvider;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_TYPE;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEY_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_TYPE;

public class Kafka3ConnectionService extends AbstractControllerService implements KafkaConnectionService, VerifiableControllerService {

    public static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("bootstrap.servers")
            .displayName("Bootstrap Servers")
            .description("Comma-separated list of Kafka Bootstrap Servers in the format host:port. Mapped to Kafka bootstrap.servers")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SecurityProtocol.values())
            .defaultValue(SecurityProtocol.PLAINTEXT.name())
            .build();

    public static final PropertyDescriptor SASL_MECHANISM = new PropertyDescriptor.Builder()
            .name("sasl.mechanism")
            .displayName("SASL Mechanism")
            .description("SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SaslMechanism.getAvailableSaslMechanisms())
            .defaultValue(SaslMechanism.GSSAPI.getValue())
            .build();

    public static final PropertyDescriptor SASL_USERNAME = new PropertyDescriptor.Builder()
            .name("sasl.username")
            .displayName("Username")
            .description("Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.PLAIN.getValue(),
                    SaslMechanism.SCRAM_SHA_256.getValue(),
                    SaslMechanism.SCRAM_SHA_512.getValue()
            )
            .build();

    public static final PropertyDescriptor SASL_PASSWORD = new PropertyDescriptor.Builder()
            .name("sasl.password")
            .displayName("Password")
            .description("Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.PLAIN.getValue(),
                    SaslMechanism.SCRAM_SHA_256.getValue(),
                    SaslMechanism.SCRAM_SHA_512.getValue()
            )
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .displayName("SSL Context Service")
            .description("Service supporting SSL communication with Kafka brokers")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder()
            .name("max.poll.records")
            .displayName("Max Poll Records")
            .description("Specifies the maximum number of records Kafka should return in a single poll.")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("default.api.timeout.ms")
            .displayName("Client Timeout")
            .description("Default timeout for Kafka client operations. Mapped to Kafka default.api.timeout.ms. The Kafka request.timeout.ms property is derived from half of the configured timeout")
            .defaultValue("60 s")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor HONOR_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("honor-transactions")
            .displayName("Honor Transactions")
            .description("Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an \"isolation level\" of "
                    + "read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If "
                    + "this value is true, NiFi will not receive any messages for which the producer's transaction was canceled, but this can result in some latency since the consumer must wait "
                    + "for the producer to finish its entire transaction instead of pulling as the messages become available.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("max.block.ms")
            .displayName("Max Metadata Wait Time")
            .description("The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the "
                    + "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("5 sec")
            .build();

    public static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("ack.wait.time")
            .displayName("Acknowledgment Wait Time")
            .description("After sending a message to Kafka, this indicates the amount of time that we are willing to wait for a response from Kafka. "
                    + "If Kafka does not acknowledge the message within this time period, the FlowFile will be routed to 'failure'.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("5 secs")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            BOOTSTRAP_SERVERS,
            SECURITY_PROTOCOL,
            SASL_MECHANISM,
            SASL_USERNAME,
            SASL_PASSWORD,
            SSL_CONTEXT_SERVICE,
            MAX_POLL_RECORDS,
            CLIENT_TIMEOUT,
            HONOR_TRANSACTIONS,
            METADATA_WAIT_TIME,
            ACK_WAIT_TIME
            //AWS_PROFILE_NAME,  // defer for now
            //KERBEROS_SERVICE_NAME,  // defer for now
            //SELF_CONTAINED_KERBEROS_USER_SERVICE,  // defer for now
            //TOKEN_AUTHENTICATION,  // defer for now
    ));

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(2);

    private static final String CONNECTION_STEP = "Kafka Broker Connection";

    private static final String TOPIC_LISTING_STEP = "Kafka Topic Listing";

    private Properties clientProperties;

    private Properties consumerProperties;

    private ServiceConfiguration serviceConfiguration;

    private Kafka3ConsumerService consumerService;

    @OnEnabled
    public void onEnabled(final ConfigurationContext configurationContext) {
        clientProperties = getClientProperties(configurationContext);
        consumerProperties = getConsumerProperties(configurationContext, clientProperties);
        serviceConfiguration = getServiceConfiguration(configurationContext);
        consumerService = new Kafka3ConsumerService(getLogger(), consumerProperties);
    }

    @OnDisabled
    public void onDisabled() {
        if (consumerService == null) {
            getLogger().warn("Consumer Service not configured");
        } else {
            consumerService.close();
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public KafkaConsumerService getConsumerService(final ConsumerConfiguration consumerConfiguration) {
        return consumerService;
    }

    @Override
    public KafkaProducerService getProducerService(final ProducerConfiguration producerConfiguration) {
        final Properties propertiesProducer = new Properties();
        propertiesProducer.putAll(clientProperties);
        if (producerConfiguration.getUseTransactions()) {
            propertiesProducer.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    new TransactionIdSupplier(producerConfiguration.getTransactionIdPrefix()).get());
        }
        if (producerConfiguration.getDeliveryGuarantee() != null) {
            propertiesProducer.put(ProducerConfig.ACKS_CONFIG, producerConfiguration.getDeliveryGuarantee());
        }
        if (producerConfiguration.getCompressionCodec() != null) {
            propertiesProducer.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerConfiguration.getCompressionCodec());
        }
        final String partitionClass = producerConfiguration.getPartitionClass();
        if (partitionClass != null && partitionClass.startsWith("org.apache.kafka")) {
            propertiesProducer.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
        }
        return new Kafka3ProducerService(propertiesProducer, serviceConfiguration, producerConfiguration);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext configurationContext, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final Properties clientProperties = getClientProperties(configurationContext);
        clientProperties.putAll(variables);
        try (final Admin admin = Admin.create(clientProperties)) {
            final ListTopicsResult listTopicsResult = admin.listTopics();

            final KafkaFuture<Collection<TopicListing>> requestedListings = listTopicsResult.listings();
            final Collection<TopicListing> topicListings = requestedListings.get(VERIFY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            final String topicListingExplanation = String.format("Topics Found [%d]", topicListings.size());
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(TOPIC_LISTING_STEP)
                            .outcome(SUCCESSFUL)
                            .explanation(topicListingExplanation)
                            .build()
            );
        } catch (final Exception e) {
            verificationLogger.error("Kafka Broker verification failed", e);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(CONNECTION_STEP)
                            .outcome(FAILED)
                            .explanation(e.getMessage())
                            .build()
            );
        }

        return results;
    }

    private Properties getConsumerProperties(final PropertyContext propertyContext, final Properties defaultProperties) {
        final Properties properties = new Properties();
        properties.putAll(defaultProperties);

        // since config for ConsumerPool is locked in at ControllerService.enable(),
        final boolean honorTransactions = propertyContext.getProperty(HONOR_TRANSACTIONS).asBoolean();
        if (honorTransactions) {
            properties.put("isolation.level", "read_committed");
        } else {
            properties.put("isolation.level", "read_uncommitted");
        }

        return properties;
    }

    private Properties getClientProperties(final PropertyContext propertyContext) {
        final Properties properties = new Properties();

        final KafkaPropertyProvider propertyProvider = new StandardKafkaPropertyProvider(ConsumerConfig.class);
        final Map<String, Object> propertiesProvider = propertyProvider.getProperties(propertyContext);
        propertiesProvider.forEach((key, value) -> properties.setProperty(key, value.toString()));

        final String configuredBootstrapServers = propertyContext.getProperty(BOOTSTRAP_SERVERS).getValue();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuredBootstrapServers);

        final String securityProtocol = propertyContext.getProperty(SECURITY_PROTOCOL).getValue();
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        final String saslMechanism = propertyContext.getProperty(SASL_MECHANISM).getValue();
        properties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

        final String saslUsername = propertyContext.getProperty(SASL_USERNAME).getValue();
        final String saslPassword = propertyContext.getProperty(SASL_PASSWORD).getValue();

        if ((saslUsername != null) && (saslPassword != null)) {
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                    "%s required username=\"%s\" password=\"%s\";",
                    PlainLoginModule.class.getName(), saslUsername, saslPassword));
        }

        setSslProperties(properties, propertyContext);

        final int defaultApiTimeoutMs = getDefaultApiTimeoutMs(propertyContext);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);

        final int requestTimeoutMs = getRequestTimeoutMs(propertyContext);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        final long timePeriod = propertyContext.getProperty(METADATA_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timePeriod);

        return properties;
    }

    private void setSslProperties(final Properties properties, final PropertyContext context) {
        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextService sslContextService = sslContextServiceProperty.asControllerService(SSLContextService.class);
            if (sslContextService.isKeyStoreConfigured()) {
                properties.put(SSL_KEYSTORE_LOCATION.getProperty(), sslContextService.getKeyStoreFile());
                properties.put(SSL_KEYSTORE_TYPE.getProperty(), sslContextService.getKeyStoreType());

                final String keyStorePassword = sslContextService.getKeyStorePassword();
                properties.put(SSL_KEYSTORE_PASSWORD.getProperty(), keyStorePassword);

                final String keyPassword = sslContextService.getKeyPassword();
                final String configuredKeyPassword = keyPassword == null ? keyStorePassword : keyPassword;
                properties.put(SSL_KEY_PASSWORD.getProperty(), configuredKeyPassword);
            }
            if (sslContextService.isTrustStoreConfigured()) {
                properties.put(SSL_TRUSTSTORE_LOCATION.getProperty(), sslContextService.getTrustStoreFile());
                properties.put(SSL_TRUSTSTORE_TYPE.getProperty(), sslContextService.getTrustStoreType());
                properties.put(SSL_TRUSTSTORE_PASSWORD.getProperty(), sslContextService.getTrustStorePassword());
            }
        }
    }

    private ServiceConfiguration getServiceConfiguration(final PropertyContext propertyContext) {
        final long maxAckWaitMillis = propertyContext.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        return new ServiceConfiguration(maxAckWaitMillis);
    }

    private int getDefaultApiTimeoutMs(final PropertyContext propertyContext) {
        return propertyContext.getProperty(CLIENT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
    }

    private int getRequestTimeoutMs(final PropertyContext propertyContext) {
        final int defaultApiTimeoutMs = getDefaultApiTimeoutMs(propertyContext);
        return defaultApiTimeoutMs / 2;
    }
}
