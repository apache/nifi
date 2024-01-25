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

package org.apache.nifi.processors.mqtt.common;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.ssl.SSLContextService;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.EnumUtils.isValidEnumIgnoreCase;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_CLEAN_SESSION_FALSE;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_CLEAN_SESSION_TRUE;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_MQTT_VERSION_310;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_MQTT_VERSION_311;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_MQTT_VERSION_500;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_MQTT_VERSION_AUTO;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_0;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_1;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_2;

public abstract class AbstractMQTTProcessor extends AbstractSessionFactoryProcessor {

    private static final String DEFAULT_SESSION_EXPIRY_INTERVAL = "24 hrs";

    protected ComponentLog logger;

    protected MqttClientProperties clientProperties;

    protected MqttClientFactory mqttClientFactory;
    protected MqttClient mqttClient;

    public ProcessSessionFactory processSessionFactory;

    public static final Validator QOS_VALIDATOR = (subject, input, context) -> {
        Integer inputInt = Integer.parseInt(input);
        if (inputInt < 0 || inputInt > 2) {
            return new ValidationResult.Builder().subject(subject).valid(false).explanation("QoS must be an integer between 0 and 2.").build();
        }
        return new ValidationResult.Builder().subject(subject).valid(true).build();
    };

    private static String getSupportedSchemeList() {
        return Arrays.stream(MqttProtocolScheme.values()).map(value -> value.name().toLowerCase()).collect(Collectors.joining(", "));
    }

    public static final Validator RETAIN_VALIDATOR = (subject, input, context) -> {
        if ("true".equalsIgnoreCase(input) || "false".equalsIgnoreCase(input)) {
            return new ValidationResult.Builder().subject(subject).valid(true).build();
        } else {
            return StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.BOOLEAN, false)
                    .validate(subject, input, context);
        }
    };

    public static final PropertyDescriptor PROP_MQTT_VERSION = new PropertyDescriptor.Builder()
            .name("MQTT Specification Version")
            .description("The MQTT specification version when connecting with the broker. See the allowable value descriptions for more details.")
            .allowableValues(
                    ALLOWABLE_VALUE_MQTT_VERSION_AUTO,
                    ALLOWABLE_VALUE_MQTT_VERSION_500,
                    ALLOWABLE_VALUE_MQTT_VERSION_311,
                    ALLOWABLE_VALUE_MQTT_VERSION_310
            )
            .defaultValue(ALLOWABLE_VALUE_MQTT_VERSION_AUTO.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_BROKER_URI = new PropertyDescriptor.Builder()
            .name("Broker URI")
            .description("Broker URI(s)")
            .description("The URI(s) to use to connect to the MQTT broker (e.g., tcp://localhost:1883). The 'tcp', 'ssl', 'ws' and 'wss' schemes are supported. " +
                    "In order to use 'ssl', the SSL Context Service property must be set. When a comma-separated URI list is set (e.g., tcp://localhost:1883,tcp://localhost:1884), " +
                    "the processor will use a round-robin algorithm to connect to the brokers on connection failure.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CLIENTID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("MQTT client ID to use. If not set, a UUID will be generated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to use when connecting to the broker")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to use when connecting to the broker")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_MESSAGE = new PropertyDescriptor.Builder()
            .name("Last Will Message")
            .description("The message to send as the client's Last Will.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_TOPIC = new PropertyDescriptor.Builder()
            .name("Last Will Topic")
            .description("The topic to send the client's Last Will to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROP_LAST_WILL_MESSAGE)
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_RETAIN = new PropertyDescriptor.Builder()
            .name("Last Will Retain")
            .description("Whether to retain the client's Last Will.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(PROP_LAST_WILL_MESSAGE)
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_QOS = new PropertyDescriptor.Builder()
            .name("Last Will QoS Level")
            .description("QoS level to be used when publishing the Last Will Message.")
            .required(true)
            .allowableValues(
                    ALLOWABLE_VALUE_QOS_0,
                    ALLOWABLE_VALUE_QOS_1,
                    ALLOWABLE_VALUE_QOS_2
            )
            .defaultValue(ALLOWABLE_VALUE_QOS_0.getValue())
            .dependsOn(PROP_LAST_WILL_MESSAGE)
            .build();

    public static final PropertyDescriptor PROP_CLEAN_SESSION = new PropertyDescriptor.Builder()
            .name("Session state")
            .description("Whether to start a fresh or resume previous flows. See the allowable value descriptions for more details.")
            .required(true)
            .allowableValues(
                    ALLOWABLE_VALUE_CLEAN_SESSION_TRUE,
                    ALLOWABLE_VALUE_CLEAN_SESSION_FALSE
            )
            .defaultValue(ALLOWABLE_VALUE_CLEAN_SESSION_TRUE.getValue())
            .build();

    public static final PropertyDescriptor PROP_SESSION_EXPIRY_INTERVAL = new PropertyDescriptor.Builder()
            .name("Session Expiry Interval")
            .description("After this interval the broker will expire the client and clear the session state.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .dependsOn(PROP_MQTT_VERSION, ALLOWABLE_VALUE_MQTT_VERSION_500)
            .dependsOn(PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE)
            .defaultValue(DEFAULT_SESSION_EXPIRY_INTERVAL)
            .build();

    public static final PropertyDescriptor PROP_CONN_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout (seconds)")
            .description("Maximum time interval the client will wait for the network connection to the MQTT server " +
                    "to be established. The default timeout is 30 seconds. " +
                    "A value of 0 disables timeout processing meaning the client will wait until the network connection is made successfully or fails.")
            .required(false)
            .defaultValue("30")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_KEEP_ALIVE_INTERVAL = new PropertyDescriptor.Builder()
            .name("Keep Alive Interval (seconds)")
            .description("Defines the maximum time interval between messages sent or received. It enables the " +
                    "client to detect if the server is no longer available, without having to wait for the TCP/IP timeout. " +
                    "The client will ensure that at least one message travels across the network within each keep alive period. In the absence of a data-related message during the time period, " +
                    "the client sends a very small \"ping\" message, which the server will acknowledge. A value of 0 disables keepalive processing in the client.")
            .required(false)
            .defaultValue("60")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASE_RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    public static final PropertyDescriptor BASE_RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(false)
            .build();

    public static final PropertyDescriptor BASE_MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("message-demarcator")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final boolean usernameSet = validationContext.getProperty(PROP_USERNAME).isSet();
        final boolean passwordSet = validationContext.getProperty(PROP_PASSWORD).isSet();

        if ((usernameSet && !passwordSet) || (!usernameSet && passwordSet)) {
            results.add(new ValidationResult.Builder().subject("Username and Password").valid(false).explanation("if username or password is set, both must be set.").build());
        }

        try {
            final List<URI> brokerUris = parseBrokerUris(validationContext.getProperty(PROP_BROKER_URI).evaluateAttributeExpressions().getValue());

            boolean sameSchemeValidationErrorAdded = false;
            boolean sslValidationErrorAdded = false;
            for(URI brokerUri : brokerUris) {
                final String scheme = brokerUri.getScheme();
                if (!isValidEnumIgnoreCase(MqttProtocolScheme.class, scheme)) {
                    results.add(new ValidationResult.Builder().subject(PROP_BROKER_URI.getName()).valid(false)
                            .explanation(scheme + " is an invalid scheme. Supported schemes are: " + getSupportedSchemeList()).build());
                }
                if (!scheme.equals(brokerUris.get(0).getScheme())) {
                    if (!sameSchemeValidationErrorAdded) {
                        results.add(new ValidationResult.Builder().subject(PROP_BROKER_URI.getName()).valid(false).explanation("all URIs should use the same scheme.").build());
                        sameSchemeValidationErrorAdded = true;
                    }
                }
                if (scheme.equalsIgnoreCase("ssl") && !validationContext.getProperty(PROP_SSL_CONTEXT_SERVICE).isSet()) {
                    if (!sslValidationErrorAdded) {
                        results.add(new ValidationResult.Builder().subject(PROP_SSL_CONTEXT_SERVICE.getName() + " or " + PROP_BROKER_URI.getName()).valid(false)
                                .explanation("if the 'ssl' scheme is used in the broker URI, the SSL Context Service must be set.").build());
                        sslValidationErrorAdded = true;
                    }
                }
            }
        } catch (Exception e) {
            results.add(new ValidationResult.Builder().subject(PROP_BROKER_URI.getName()).valid(false)
                    .explanation("it is not valid URI syntax.").build());
        }

        final boolean readerIsSet = validationContext.getProperty(BASE_RECORD_READER).isSet();
        final boolean writerIsSet = validationContext.getProperty(BASE_RECORD_WRITER).isSet();
        if ((readerIsSet && !writerIsSet) || (!readerIsSet && writerIsSet)) {
            results.add(new ValidationResult.Builder().subject("Record Reader and Writer").valid(false)
                    .explanation("both properties must be set when used.").build());
        }

        final boolean demarcatorIsSet = validationContext.getProperty(BASE_MESSAGE_DEMARCATOR).isSet();
        if (readerIsSet && demarcatorIsSet) {
            results.add(new ValidationResult.Builder().subject("Reader and Writer").valid(false)
                    .explanation("Message Demarcator and Record Reader/Writer cannot be used at the same time.").build());
        }

        return results;
    }

    protected void onScheduled(final ProcessContext context) {
        clientProperties = getMqttClientProperties(context);
        mqttClientFactory = new MqttClientFactory(clientProperties, logger);
    }

    protected void stopClient() {
        // Since client is created in the onTrigger method it can happen that it never will be created because of an initialization error.
        // We are preventing additional nullPtrException here, but the clean solution would be to create the client in the onScheduled method.
        if (mqttClient != null) {
            try {
                logger.info("Disconnecting client");
                mqttClient.disconnect();
            } catch (Exception e) {
                logger.error("Error disconnecting MQTT client", e);
            }

            try {
                logger.info("Closing client");
                mqttClient.close();
            } catch (Exception e) {
                logger.error("Error closing MQTT client", e);
            }

            mqttClient = null;
        }
    }

    protected MqttClient createMqttClient() throws TlsException {
        return mqttClientFactory.create();
    }


    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (processSessionFactory == null) {
            processSessionFactory = sessionFactory;
        }
        ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commitAsync();
        } catch (final Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back session", this, t);
            session.rollback(true);
            throw t;
        }
    }

    public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

    protected boolean isConnected() {
        return (mqttClient != null && mqttClient.isConnected());
    }

    protected MqttClientProperties getMqttClientProperties(final ProcessContext context) {
        final MqttClientProperties clientProperties = new MqttClientProperties();

        final String rawBrokerUris = context.getProperty(PROP_BROKER_URI).evaluateAttributeExpressions().getValue();
        clientProperties.setBrokerUris(parseBrokerUris(rawBrokerUris));

        String clientId = context.getProperty(PROP_CLIENTID).evaluateAttributeExpressions().getValue();
        if (clientId == null) {
            clientId = UUID.randomUUID().toString();
        }
        clientProperties.setClientId(clientId);

        clientProperties.setMqttVersion(MqttVersion.fromVersionCode(context.getProperty(PROP_MQTT_VERSION).asInteger()));

        clientProperties.setCleanSession(context.getProperty(PROP_CLEAN_SESSION).asBoolean());
        clientProperties.setSessionExpiryInterval(context.getProperty(PROP_SESSION_EXPIRY_INTERVAL).asTimePeriod(TimeUnit.SECONDS));

        clientProperties.setKeepAliveInterval(context.getProperty(PROP_KEEP_ALIVE_INTERVAL).asInteger());
        clientProperties.setConnectionTimeout(context.getProperty(PROP_CONN_TIMEOUT).asInteger());

        final SSLContextService sslContextService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            clientProperties.setTlsConfiguration(sslContextService.createTlsConfiguration());
        }

        if (context.getProperty(PROP_LAST_WILL_MESSAGE).isSet()) {
            clientProperties.setLastWillMessage(context.getProperty(PROP_LAST_WILL_MESSAGE).getValue());
            clientProperties.setLastWillTopic(context.getProperty(PROP_LAST_WILL_TOPIC).getValue());
            clientProperties.setLastWillRetain(context.getProperty(PROP_LAST_WILL_RETAIN).asBoolean());
            clientProperties.setLastWillQos(context.getProperty(PROP_LAST_WILL_QOS).asInteger());
        }

        final PropertyValue usernameProp = context.getProperty(PROP_USERNAME);
        if (usernameProp.isSet()) {
            clientProperties.setUsername(usernameProp.evaluateAttributeExpressions().getValue());
        }

        clientProperties.setPassword(context.getProperty(PROP_PASSWORD).getValue());

        return clientProperties;
    }

    private static List<URI> parseBrokerUris(String brokerUris) {
        final List<URI> uris = Pattern.compile(",").splitAsStream(brokerUris)
                .map(AbstractMQTTProcessor::parseUri)
                .collect(Collectors.toList());
        return uris;
    }

    private static URI parseUri(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid Broker URI", e);
        }
    }
}
