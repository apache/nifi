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
import org.apache.nifi.ssl.SSLContextService;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.EnumUtils.isValidEnumIgnoreCase;
import static org.apache.commons.lang3.StringUtils.EMPTY;
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

    protected MqttClientFactory mqttClientFactory = new MqttClientFactory();
    protected MqttClient mqttClient;

    public ProcessSessionFactory processSessionFactory;

    public static final Validator QOS_VALIDATOR = (subject, input, context) -> {
        Integer inputInt = Integer.parseInt(input);
        if (inputInt < 0 || inputInt > 2) {
            return new ValidationResult.Builder().subject(subject).valid(false).explanation("QoS must be an integer between 0 and 2.").build();
        }
        return new ValidationResult.Builder().subject(subject).valid(true).build();
    };

    public static final Validator BROKER_VALIDATOR = (subject, input, context) -> {
        try {
            URI brokerURI = new URI(input);
            if (!EMPTY.equals(brokerURI.getPath())) {
                return new ValidationResult.Builder().subject(subject).valid(false).explanation("the broker URI cannot have a path. It currently is: " + brokerURI.getPath()).build();
            }
            if (!isValidEnumIgnoreCase(MqttProtocolScheme.class, brokerURI.getScheme())) {
                return new ValidationResult.Builder().subject(subject).valid(false)
                        .explanation("scheme is invalid. Supported schemes are: " + getSupportedSchemeList()).build();
            }
        } catch (URISyntaxException e) {
            return new ValidationResult.Builder().subject(subject).valid(false).explanation("it is not valid URI syntax.").build();
        }
        return new ValidationResult.Builder().subject(subject).valid(true).build();
    };

    private static String getSupportedSchemeList() {
        return String.join(", ", Arrays.stream(MqttProtocolScheme.values()).map(value -> value.name().toLowerCase()).toArray(String[]::new));
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
            .description("The URI to use to connect to the MQTT broker (e.g. tcp://localhost:1883). The 'tcp', 'ssl', 'ws' and 'wss' schemes are supported. In order to use 'ssl', the SSL Context " +
                    "Service property must be set.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(BROKER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CLIENTID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("MQTT client ID to use. If not set, a UUID will be generated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to use when connecting to the broker")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

    public static final PropertyDescriptor PROP_LAST_WILL_TOPIC = new PropertyDescriptor.Builder()
            .name("Last Will Topic")
            .description("The topic to send the client's Last Will to. If the Last Will topic and message are not set then a Last Will will not be sent.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_MESSAGE = new PropertyDescriptor.Builder()
            .name("Last Will Message")
            .description("The message to send as the client's Last Will. If the Last Will topic and message are not set then a Last Will will not be sent.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_RETAIN = new PropertyDescriptor.Builder()
            .name("Last Will Retain")
            .description("Whether to retain the client's Last Will. If the Last Will topic and message are not set then a Last Will will not be sent.")
            .required(false)
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_LAST_WILL_QOS = new PropertyDescriptor.Builder()
            .name("Last Will QoS Level")
            .description("QoS level to be used when publishing the Last Will Message")
            .required(false)
            .allowableValues(
                    ALLOWABLE_VALUE_QOS_0,
                    ALLOWABLE_VALUE_QOS_1,
                    ALLOWABLE_VALUE_QOS_2
            )
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

    public static List<PropertyDescriptor> getAbstractPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_BROKER_URI);
        descriptors.add(PROP_CLIENTID);
        descriptors.add(PROP_USERNAME);
        descriptors.add(PROP_PASSWORD);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_LAST_WILL_TOPIC);
        descriptors.add(PROP_LAST_WILL_MESSAGE);
        descriptors.add(PROP_LAST_WILL_RETAIN);
        descriptors.add(PROP_LAST_WILL_QOS);
        descriptors.add(PROP_CLEAN_SESSION);
        descriptors.add(PROP_SESSION_EXPIRY_INTERVAL);
        descriptors.add(PROP_MQTT_VERSION);
        descriptors.add(PROP_CONN_TIMEOUT);
        descriptors.add(PROP_KEEP_ALIVE_INTERVAL);
        return descriptors;
    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final boolean usernameSet = validationContext.getProperty(PROP_USERNAME).isSet();
        final boolean passwordSet = validationContext.getProperty(PROP_PASSWORD).isSet();

        if ((usernameSet && !passwordSet) || (!usernameSet && passwordSet)) {
            results.add(new ValidationResult.Builder().subject("Username and Password").valid(false).explanation("if username or password is set, both must be set.").build());
        }

        final boolean lastWillTopicSet = validationContext.getProperty(PROP_LAST_WILL_TOPIC).isSet();
        final boolean lastWillMessageSet = validationContext.getProperty(PROP_LAST_WILL_MESSAGE).isSet();

        final boolean lastWillRetainSet = validationContext.getProperty(PROP_LAST_WILL_RETAIN).isSet();
        final boolean lastWillQosSet = validationContext.getProperty(PROP_LAST_WILL_QOS).isSet();

        // If any of the Last Will Properties are set
        if (lastWillTopicSet || lastWillMessageSet || lastWillRetainSet || lastWillQosSet) {
            // And any are not set
            if (!(lastWillTopicSet && lastWillMessageSet && lastWillRetainSet && lastWillQosSet)) {
                // Then mark as invalid
                results.add(new ValidationResult.Builder().subject("Last Will Properties").valid(false).explanation("if any of the Last Will Properties (message, topic, retain and QoS) are " +
                        "set, all must be set.").build());
            }
        }

        try {
            URI brokerURI = new URI(validationContext.getProperty(PROP_BROKER_URI).evaluateAttributeExpressions().getValue());
            if (brokerURI.getScheme().equalsIgnoreCase("ssl") && !validationContext.getProperty(PROP_SSL_CONTEXT_SERVICE).isSet()) {
                results.add(new ValidationResult.Builder().subject(PROP_SSL_CONTEXT_SERVICE.getName() + " or " + PROP_BROKER_URI.getName()).valid(false).explanation("if the 'ssl' scheme is used in " +
                        "the broker URI, the SSL Context Service must be set.").build());
            }
        } catch (URISyntaxException e) {
            results.add(new ValidationResult.Builder().subject(PROP_BROKER_URI.getName()).valid(false).explanation("it is not valid URI syntax.").build());
        }

        return results;
    }

    protected void onScheduled(final ProcessContext context) {
        clientProperties = getMqttClientProperties(context);
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
        return mqttClientFactory.create(clientProperties, getLogger());
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

        try {
            clientProperties.setBrokerUri(new URI(context.getProperty(PROP_BROKER_URI).evaluateAttributeExpressions().getValue()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid Broker URI", e);
        }

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

        final PropertyValue sslProp = context.getProperty(PROP_SSL_CONTEXT_SERVICE);
        if (sslProp.isSet()) {
            final SSLContextService sslContextService = (SSLContextService) sslProp.asControllerService();
            clientProperties.setTlsConfiguration(sslContextService.createTlsConfiguration());
        }

        clientProperties.setLastWillTopic(context.getProperty(PROP_LAST_WILL_TOPIC).getValue());
        clientProperties.setLastWillMessage(context.getProperty(PROP_LAST_WILL_MESSAGE).getValue());
        final PropertyValue lastWillRetain = context.getProperty(PROP_LAST_WILL_RETAIN);
        clientProperties.setLastWillRetain(lastWillRetain.isSet() ? lastWillRetain.asBoolean() : false);
        clientProperties.setLastWillQos(context.getProperty(PROP_LAST_WILL_QOS).asInteger());

        final PropertyValue usernameProp = context.getProperty(PROP_USERNAME);
        if (usernameProp.isSet()) {
            clientProperties.setUsername(usernameProp.evaluateAttributeExpressions().getValue());
        }

        clientProperties.setPassword(context.getProperty(PROP_PASSWORD).getValue());

        return clientProperties;
    }
}
