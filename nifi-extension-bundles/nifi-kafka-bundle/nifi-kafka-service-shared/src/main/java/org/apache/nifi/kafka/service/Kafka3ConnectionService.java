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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.consumer.Kafka3ConsumerService;
import org.apache.nifi.kafka.service.consumer.Subscription;
import org.apache.nifi.kafka.service.producer.Kafka3ProducerService;
import org.apache.nifi.kafka.service.security.OAuthBearerLoginCallbackHandler;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.IsolationLevel;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.provider.KafkaPropertyProvider;
import org.apache.nifi.kafka.shared.property.provider.StandardKafkaPropertyProvider;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.kafka.shared.validation.DynamicPropertyValidator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.nifi.kafka.service.security.OAuthBearerLoginCallbackHandler.PROPERTY_KEY_NIFI_OAUTH_2_ACCESS_TOKEN_PROVIDER;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_TYPE;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEY_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_TYPE;
import static org.apache.nifi.kafka.shared.util.SaslExtensionUtil.SASL_EXTENSION_PROPERTY_PREFIX;
import static org.apache.nifi.kafka.shared.util.SaslExtensionUtil.isSaslExtensionProperty;
import static org.apache.nifi.kafka.shared.util.SaslExtensionUtil.removeSaslExtensionPropertyPrefix;

@Tags({"Apache", "Kafka", "Message", "Publish", "Consume"})
@DynamicProperty(name = "The name of a Kafka configuration property or a SASL extension property.", value = "The value of the given property.",
        description = "Kafka configuration properties will be added on the Kafka configuration after loading any provided configuration properties."
                + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
                + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration."
                + " SASL extension properties can be specified in " + SASL_EXTENSION_PROPERTY_PREFIX + "propertyName format (e.g. " + SASL_EXTENSION_PROPERTY_PREFIX + "logicalCluster).",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
@CapabilityDescription("Provides and manages connections to Kafka Brokers for producer or consumer operations.")
public class Kafka3ConnectionService extends AbstractControllerService implements KafkaConnectionService, VerifiableControllerService, KafkaClientComponent {

    public static final PropertyDescriptor TRANSACTION_ISOLATION_LEVEL = new PropertyDescriptor.Builder()
            .name("isolation.level")
            .displayName("Transaction Isolation Level")
            .description("""
                    Specifies how the service should handle transaction isolation levels when communicating with Kafka.
                    The uncommited option means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions.
                    The committed option configures the service to not receive any messages for which the producer's transaction was canceled, but this can result in some latency since the
                    consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available.
                    Corresponds to Kafka isolation.level property.
                    """)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(IsolationLevel.class)
            .defaultValue(IsolationLevel.READ_COMMITTED)
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder()
            .name("max.poll.records")
            .displayName("Max Poll Records")
            .description("Maximum number of records Kafka should return in a single poll.")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("default.api.timeout.ms")
            .displayName("Client Timeout")
            .description("Default timeout for Kafka client operations. Mapped to Kafka default.api.timeout.ms. The Kafka request.timeout.ms property is derived from half of the configured timeout")
            .defaultValue("60 sec")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("max.block.ms")
            .displayName("Max Metadata Wait Time")
            .description("""
                    The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the
                    entire 'send' call. Corresponds to Kafka max.block.ms property
                    """)
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("5 sec")
            .build();

    public static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("ack.wait.time")
            .displayName("Acknowledgment Wait Time")
            .description("""
                    After sending a message to Kafka, this indicates the amount of time that the service will wait for a response from Kafka.
                    If Kafka does not acknowledge the message within this time period, the service will throw an exception.
                    """)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("5 sec")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOOTSTRAP_SERVERS,
            SECURITY_PROTOCOL,
            SASL_MECHANISM,
            SASL_USERNAME,
            SASL_PASSWORD,
            OAUTH2_ACCESS_TOKEN_PROVIDER_SERVICE,
            SELF_CONTAINED_KERBEROS_USER_SERVICE,
            KERBEROS_SERVICE_NAME,
            SSL_CONTEXT_SERVICE,
            TRANSACTION_ISOLATION_LEVEL,
            MAX_POLL_RECORDS,
            CLIENT_TIMEOUT,
            METADATA_WAIT_TIME,
            ACK_WAIT_TIME
    );

    private static final KafkaConnectionVerifier kafkaConnectionVerifier = new KafkaConnectionVerifier();

    private volatile ServiceConfiguration serviceConfiguration;
    private volatile Properties producerProperties;
    private volatile Properties consumerProperties;
    private volatile String uri;

    @OnEnabled
    public void onEnabled(final ConfigurationContext configurationContext) {
        final Properties clientProperties = getClientProperties(configurationContext);
        serviceConfiguration = getServiceConfiguration(configurationContext);
        producerProperties = getProducerProperties(configurationContext, clientProperties);
        consumerProperties = getConsumerProperties(configurationContext, clientProperties);
        uri = createBrokerUri(configurationContext);
    }

    private String createBrokerUri(final ConfigurationContext context) {
        final String bootstrapServers = context.getProperty(BOOTSTRAP_SERVERS).getValue();
        final String[] bootstrapServersArray = bootstrapServers.split(",");
        String firstBootstrapServer = bootstrapServersArray[0].trim();
        if (firstBootstrapServer.contains("://")) {
            firstBootstrapServer = firstBootstrapServer.substring(firstBootstrapServer.indexOf("://") + 3);
        }

        final SecurityProtocol securityProtocol = context.getProperty(SECURITY_PROTOCOL).asAllowableValue(SecurityProtocol.class);
        final String protocol = switch (securityProtocol) {
            case SSL, SASL_SSL -> "kafkas";
            case SASL_PLAINTEXT, PLAINTEXT -> "kafka";
        };

        return String.format("%s://%s", protocol, firstBootstrapServer);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final String propertyName;
        final String propertyType;
        final ExpressionLanguageScope expressionLanguageScope;

        if (isSaslExtensionProperty(propertyDescriptorName)) {
            propertyName = removeSaslExtensionPropertyPrefix(propertyDescriptorName);
            propertyType = "SASL Extension";
            expressionLanguageScope = ExpressionLanguageScope.NONE;
        } else {
            propertyName = propertyDescriptorName;
            propertyType = "Kafka Configuration";
            expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT;
        }

        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '%s' %s property.".formatted(propertyName, propertyType))
                .name(propertyDescriptorName)
                .addValidator(new DynamicPropertyValidator(ProducerConfig.class, ConsumerConfig.class))
                .dynamic(true)
                .expressionLanguageSupported(expressionLanguageScope)
                .build();
    }

    @Override
    public KafkaConsumerService getConsumerService(final PollingContext pollingContext) {
        Objects.requireNonNull(pollingContext, "Polling Context required");

        final Subscription subscription = createSubscription(pollingContext);

        final Properties properties = new Properties();
        properties.putAll(consumerProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, subscription.getGroupId());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, subscription.getAutoOffsetReset().getValue());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, deserializer, deserializer);

        return new Kafka3ConsumerService(getLogger(), consumer, subscription);
    }

    private Subscription createSubscription(final PollingContext pollingContext) {
        final String groupId = pollingContext.getGroupId();
        final Optional<Pattern> topicPatternFound = pollingContext.getTopicPattern();
        final AutoOffsetReset autoOffsetReset = pollingContext.getAutoOffsetReset();

        return topicPatternFound
            .map(pattern -> new Subscription(groupId, pattern, autoOffsetReset))
            .orElseGet(() -> new Subscription(groupId, pollingContext.getTopics(), autoOffsetReset));
    }

    @Override
    public KafkaProducerService getProducerService(final ProducerConfiguration producerConfiguration) {
        final Properties properties = new Properties();
        properties.putAll(producerProperties);
        if (producerConfiguration.getTransactionsEnabled()) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    new TransactionIdSupplier(producerConfiguration.getTransactionIdPrefix()).get());
        }
        if (producerConfiguration.getDeliveryGuarantee() != null) {
            properties.put(ProducerConfig.ACKS_CONFIG, producerConfiguration.getDeliveryGuarantee());
        }
        if (producerConfiguration.getCompressionCodec() != null) {
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerConfiguration.getCompressionCodec());
        }
        final String partitionClass = producerConfiguration.getPartitionClass();
        // Default Partitioner is removed in Kafka 4.0, and partitioner class should be
        // null by default - see KIP-794
        if (partitionClass != null && partitionClass.startsWith("org.apache.kafka")
                && !partitionClass.equals("org.apache.kafka.clients.producer.internals.DefaultPartitioner")) {
            properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
        }
        // because this property is always set from the processor properties, and has a default, we set it here unconditionally
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerConfiguration.getMaxRequestSize());

        return new Kafka3ProducerService(properties, serviceConfiguration, producerConfiguration);
    }

    @Override
    public String getBrokerUri() {
        return uri;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext configurationContext, final ComponentLog verificationLogger, final Map<String, String> variables) {
        // Build Client Properties based on configured values and defaults from Consumer Properties
        final Properties clientProperties = getClientProperties(configurationContext);
        final Properties consumerProperties = getConsumerProperties(configurationContext, clientProperties);
        consumerProperties.putAll(variables);

        return kafkaConnectionVerifier.verify(verificationLogger, consumerProperties);
    }

    protected Properties getProducerProperties(final PropertyContext propertyContext, final Properties defaultProperties) {
        final Properties properties = new Properties();
        properties.putAll(defaultProperties);

        final KafkaPropertyProvider propertyProvider = new StandardKafkaPropertyProvider(ProducerConfig.class);
        final Map<String, Object> propertiesProvider = propertyProvider.getProperties(propertyContext);
        propertiesProvider.forEach((key, value) -> properties.setProperty(key, value.toString()));

        final long timePeriod = propertyContext.getProperty(METADATA_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timePeriod);

        return properties;
    }

    protected Properties getConsumerProperties(final PropertyContext propertyContext, final Properties defaultProperties) {
        final Properties properties = new Properties();
        properties.putAll(defaultProperties);

        final KafkaPropertyProvider propertyProvider = new StandardKafkaPropertyProvider(ConsumerConfig.class);
        final Map<String, Object> propertiesProvider = propertyProvider.getProperties(propertyContext);
        propertiesProvider.forEach((key, value) -> properties.setProperty(key, value.toString()));

        final IsolationLevel isolationLevel = propertyContext.getProperty(TRANSACTION_ISOLATION_LEVEL).asAllowableValue(IsolationLevel.class);
        properties.put(TRANSACTION_ISOLATION_LEVEL.getName(), isolationLevel.getValue());

        return properties;
    }

    protected Properties getClientProperties(final PropertyContext propertyContext) {
        final Properties properties = new Properties();

        final String configuredBootstrapServers = propertyContext.getProperty(BOOTSTRAP_SERVERS).getValue();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuredBootstrapServers);

        setOAuthProperties(properties, propertyContext);

        setSslProperties(properties, propertyContext);

        final int defaultApiTimeoutMs = getDefaultApiTimeoutMs(propertyContext);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);

        final int requestTimeoutMs = getRequestTimeoutMs(propertyContext);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        return properties;
    }

    private void setOAuthProperties(Properties properties, PropertyContext propertyContext) {
        final SecurityProtocol securityProtocol = propertyContext.getProperty(SECURITY_PROTOCOL).asAllowableValue(SecurityProtocol.class);
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            final SaslMechanism saslMechanism = propertyContext.getProperty(SASL_MECHANISM).asAllowableValue(SaslMechanism.class);
            if (saslMechanism == SaslMechanism.OAUTHBEARER) {
                properties.put(SASL_LOGIN_CALLBACK_HANDLER_CLASS.getProperty(), OAuthBearerLoginCallbackHandler.class.getName());
                final OAuth2AccessTokenProvider accessTokenProvider = propertyContext.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER_SERVICE).asControllerService(OAuth2AccessTokenProvider.class);
                properties.put(PROPERTY_KEY_NIFI_OAUTH_2_ACCESS_TOKEN_PROVIDER, accessTokenProvider);
            }
        }
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
        final Duration maxAckWait = propertyContext.getProperty(ACK_WAIT_TIME).asDuration();
        return new ServiceConfiguration(maxAckWait);
    }

    private int getDefaultApiTimeoutMs(final PropertyContext propertyContext) {
        return propertyContext.getProperty(CLIENT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
    }

    private int getRequestTimeoutMs(final PropertyContext propertyContext) {
        final int defaultApiTimeoutMs = getDefaultApiTimeoutMs(propertyContext);
        return defaultApiTimeoutMs / 2;
    }
}
