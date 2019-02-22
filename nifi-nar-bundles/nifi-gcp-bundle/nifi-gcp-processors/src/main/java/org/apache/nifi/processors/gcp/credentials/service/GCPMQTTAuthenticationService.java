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
package org.apache.nifi.processors.gcp.credentials.service;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.mqtt.services.MQTTAuthenticationService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of MQTTAuthenticationService interface
 *
 * @see MQTTAuthenticationService
 */
@CapabilityDescription("Defines the authentication MQTT connection options to connect with GCP IoT Core using the MQTT bridge.")
@Tags({ "gcp", "google", "iot", "mqtt", "credentials", "provider" })
public class GCPMQTTAuthenticationService extends AbstractControllerService implements MQTTAuthenticationService {

    private static final List<PropertyDescriptor> properties;

    public static final PropertyDescriptor PROP_PROJECTID = new PropertyDescriptor.Builder()
            .displayName("Project ID")
            .name("gcp-mqtt-project-id")
            .description("Project ID of the Google IoT Core MQTT server to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PRIVATE_KEY_FILE = new PropertyDescriptor.Builder()
            .name("Private Key")
            .description("Path to the private key of the device")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Algorithm")
            .description("Algorithm used for the private key")
            .required(true)
            .allowableValues("RS256", "ES256")
            .build();

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_PROJECTID);
        props.add(PROP_PRIVATE_KEY_FILE);
        props.add(PROP_ALGORITHM);
        properties = Collections.unmodifiableList(props);
    }

    private volatile String projectId;
    private volatile String privateKeyFile;
    private volatile String algorithm;
    private volatile DateTime lastRefresh;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        projectId = context.getProperty(PROP_PROJECTID).getValue();
        privateKeyFile = context.getProperty(PROP_PRIVATE_KEY_FILE).getValue();
        algorithm = context.getProperty(PROP_ALGORITHM).getValue();
    }

    private String createJwtRsa(String projectId, String privateKeyFile) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        DateTime now = new DateTime();
        // Create a JWT to authenticate this device. The device will be disconnected after the token
        // expires, and will have to reconnect with a new token. The audience field should always be set
        // to the GCP project id.
        JwtBuilder jwtBuilder = Jwts.builder()
                .setIssuedAt(now.toDate())
                .setExpiration(now.plusMinutes(20).toDate())
                .setAudience(projectId);

        byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");

        return jwtBuilder.signWith(SignatureAlgorithm.RS256, kf.generatePrivate(spec)).compact();
    }

    private String createJwtEs(String projectId, String privateKeyFile) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        DateTime now = new DateTime();
        // Create a JWT to authenticate this device. The device will be disconnected after the token
        // expires, and will have to reconnect with a new token. The audience field should always be set
        // to the GCP project id.
        JwtBuilder jwtBuilder =
                Jwts.builder()
                .setIssuedAt(now.toDate())
                .setExpiration(now.plusMinutes(20).toDate())
                .setAudience(projectId);

        byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("EC");

        return jwtBuilder.signWith(SignatureAlgorithm.ES256, kf.generatePrivate(spec)).compact();
    }

    @Override
    public void setMqttConnectOptions(MqttConnectOptions connOpts) throws ProcessException {
        try {

            // Note that the Google Cloud IoT Core only supports MQTT 3.1.1, and Paho requires that we
            // explicitly set this. If you don't set MQTT version, the server will immediately close its
            // connection to your device.
            connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);

            Properties sslProps = new Properties();
            sslProps.setProperty("com.ibm.ssl.protocol", "TLSv1.2");
            connOpts.setSSLProperties(sslProps);

            // With Google Cloud IoT Core, the user name field is ignored, however it must be set for the
            // Paho client library to send the password field. The password field is used to transmit a JWT
            // to authorize the device.
            connOpts.setUserName("unused");

            if (algorithm.equals("RS256")) {
                connOpts.setPassword(createJwtRsa(projectId, privateKeyFile).toCharArray());
            } else if (algorithm.equals("ES256")) {
                connOpts.setPassword(createJwtEs(projectId, privateKeyFile).toCharArray());
            } else {
                throw new IllegalArgumentException("Invalid algorithm " + algorithm + ". Should be one of 'RS256' or 'ES256'.");
            }

            lastRefresh = new DateTime();
        } catch (InvalidKeySpecException | NoSuchAlgorithmException | IOException e) {
            throw new ProcessException("Error while setting authentication properties for the MQTT connection", e);
        }
    }

    @Override
    public void refreshConnectOptions(MqttConnectOptions connOpts) throws ProcessException {
        DateTime now = new DateTime();
        if(lastRefresh.plusMinutes(15).isBefore(now)) {
            try {
                if (algorithm.equals("RS256")) {
                    connOpts.setPassword(createJwtRsa(projectId, privateKeyFile).toCharArray());
                } else if (algorithm.equals("ES256")) {
                    connOpts.setPassword(createJwtEs(projectId, privateKeyFile).toCharArray());
                } else {
                    throw new IllegalArgumentException("Invalid algorithm " + algorithm + ". Should be one of 'RS256' or 'ES256'.");
                }
            } catch (InvalidKeySpecException | NoSuchAlgorithmException | IOException e) {
                throw new ProcessException("Error while setting authentication properties for the MQTT connection", e);
            }
            lastRefresh = now;
        }
    }

    @Override
    public String toString() {
        return "GCPMQTTAuthenticationService[id=" + getIdentifier() + "]";
    }

}