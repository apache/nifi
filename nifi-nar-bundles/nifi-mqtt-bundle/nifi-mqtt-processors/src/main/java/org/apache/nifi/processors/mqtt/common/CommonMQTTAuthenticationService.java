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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.mqtt.services.MQTTAuthenticationService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of MQTTAuthenticationService interface for common usage in order to remove deprecated properties in next major version
 *
 * @see MQTTAuthenticationService
 */
@CapabilityDescription("Defines the authentication MQTT connection options")
@Tags({ "iot", "mqtt", "credentials", "provider" })
public class CommonMQTTAuthenticationService extends AbstractControllerService implements MQTTAuthenticationService {

    private static final List<PropertyDescriptor> properties;

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to use when connecting to the broker")
            .required(false)
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

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_USERNAME);
        props.add(PROP_PASSWORD);
        props.add(PROP_SSL_CONTEXT_SERVICE);
        properties = Collections.unmodifiableList(props);
    }

    private volatile String username;
    private volatile String password;
    private volatile SSLContextService sslContextService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final boolean usernameSet = validationContext.getProperty(PROP_USERNAME).isSet();
        final boolean passwordSet = validationContext.getProperty(PROP_PASSWORD).isSet();

        if ((usernameSet && !passwordSet) || (!usernameSet && passwordSet)) {
            results.add(new ValidationResult.Builder().subject("Username and Password").valid(false).explanation("if username or password is set, both must be set").build());
        }

        return results;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        username = context.getProperty(PROP_USERNAME).getValue();
        password = context.getProperty(PROP_PASSWORD).getValue();
        sslContextService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
    }

    private Properties transformSSLContextService(SSLContextService sslContextService){
        Properties properties = new Properties();
        properties.setProperty("com.ibm.ssl.protocol", sslContextService.getSslAlgorithm());
        properties.setProperty("com.ibm.ssl.keyStore", sslContextService.getKeyStoreFile());
        properties.setProperty("com.ibm.ssl.keyStorePassword", sslContextService.getKeyStorePassword());
        properties.setProperty("com.ibm.ssl.keyStoreType", sslContextService.getKeyStoreType());
        properties.setProperty("com.ibm.ssl.trustStore", sslContextService.getTrustStoreFile());
        properties.setProperty("com.ibm.ssl.trustStorePassword", sslContextService.getTrustStorePassword());
        properties.setProperty("com.ibm.ssl.trustStoreType", sslContextService.getTrustStoreType());
        return  properties;
    }

    @Override
    public void setMqttConnectOptions(MqttConnectOptions connOpts) throws ProcessException {

        if (sslContextService != null) {
            Properties sslProps = transformSSLContextService(sslContextService);
            connOpts.setSSLProperties(sslProps);
        }

        if(username != null) {
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
        }

    }

    @Override
    public String toString() {
        return "CommonMQTTAuthenticationService[id=" + getIdentifier() + "]";
    }

    @Override
    public void refreshConnectOptions(MqttConnectOptions connOpts) throws ProcessException {
        return;
    }
}
