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
package org.apache.nifi.processors.aws.iot;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.iot.AWSIotClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.iot.util.AWS4Signer;
import org.apache.nifi.processors.aws.iot.util.MqttWebSocketAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public abstract class AbstractIOTMqttProcessor extends AbstractAWSCredentialsProviderProcessor<AWSIotClient> {
    protected static final String PROP_NAME_ENDPOINT = "aws.iot.endpoint";
    protected static final String PROP_NAME_CLIENT = "aws.iot.mqtt.client";
    protected static final String PROP_NAME_KEEPALIVE = "aws.iot.mqtt.keepalive";
    protected static final String PROP_NAME_TOPIC = "aws.iot.mqtt.topic";
    protected static final String PROP_NAME_QOS = "aws.iot.mqtt.qos";
    protected static final Integer PROP_DEFAULT_KEEPALIVE = 300;
    protected static final String PROP_DEFAULT_CLIENT = AbstractIOTMqttProcessor.class.getSimpleName();
    protected static final Integer DEFAULT_CONNECTION_RENEWAL_BEFORE_KEEP_ALIVE_EXPIRATION = 20;
    protected static final Integer DEFAULT_QOS = 0;
    protected String awsTopic;
    protected int awsQos;
    protected MqttWebSocketAsyncClient mqttClient;
    protected String awsEndpoint;
    protected String awsClientId;

    private String awsRegion;
    private Integer awsKeepAliveSeconds;
    private Date dtLastConnect;

    public static final PropertyDescriptor PROP_ENDPOINT = new PropertyDescriptor
            .Builder().name(PROP_NAME_ENDPOINT)
            .description("Your endpoint identifier in AWS IoT (e.g. A1B71MLXKNCXXX)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CLIENT = new PropertyDescriptor
            .Builder().name(PROP_NAME_CLIENT)
            .description("MQTT client ID to use. Under the cover your input will be extended by a random string to ensure a unique id among all conntected clients.")
            .required(false)
            .defaultValue(PROP_DEFAULT_CLIENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_KEEPALIVE = new PropertyDescriptor
            .Builder().name(PROP_NAME_KEEPALIVE)
            .description("Seconds a WebSocket-connection remains open after automatically renewing it. This is neccessary due to Amazon's service limit on WebSocket connection duration. As soon as the limit is changed by Amazon you can adjust the value here. Never use a duration longer than supported by Amazon. This processor renews the connection 30 seconds before the actual expiration. If no value set the default will be " + PROP_DEFAULT_KEEPALIVE + ".")
            .required(false)
            .defaultValue(PROP_DEFAULT_KEEPALIVE.toString())
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TOPIC = new PropertyDescriptor
            .Builder().name(PROP_NAME_TOPIC)
            .description("MQTT topic to work with. (pattern: $aws/things/mything/shadow/update).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_QOS = new PropertyDescriptor
            .Builder().name(PROP_NAME_QOS)
            .description("Decide for at most once (0) or at least once (1) message-receiption. " +
                    "Currently AWS IoT does not support QoS-level 2. If no value set the default QoS is " + DEFAULT_QOS + ".")
            .required(false)
            .allowableValues("0", "1")
            .defaultValue("0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AWSIotClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client using aws credentials provider ");

        return new AWSIotClient(credentialsProvider, config);
    }

    /**
     * Create client using AWSCredentails
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AWSIotClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client using aws credentials ");

        return new AWSIotClient(credentials, config);
    }

    protected void init(final ProcessContext context) {
        awsEndpoint = context.getProperty(PROP_ENDPOINT).getValue();
        awsRegion = context.getProperty(REGION).getValue();
        awsClientId = context.getProperty(PROP_CLIENT).isSet() ? context.getProperty(PROP_CLIENT).getValue() : PROP_DEFAULT_CLIENT;
        awsKeepAliveSeconds = context.getProperty(PROP_KEEPALIVE).isSet() ? context.getProperty(PROP_KEEPALIVE).asInteger() : PROP_DEFAULT_KEEPALIVE;
        awsTopic = context.getProperty(PROP_TOPIC).getValue();
        awsQos = context.getProperty(PROP_QOS).isSet() ? context.getProperty(PROP_QOS).asInteger() : DEFAULT_QOS;
        // initialize and connect to mqtt endpoint
        mqttClient = connect(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        try {
            mqttClient.disconnect();
        } catch (MqttException me) {
            getLogger().warn("MQTT " + me.getMessage());
        }
        getLogger().info("Disconnected");
    }

    /**
     * Returns the lifetime-seconds of the established websocket-connection
     *
     * @return Lifetime-seconds of websocket-connection
     */
    protected long getConnectionDuration() {
        return dtLastConnect != null ?
                TimeUnit.MILLISECONDS.toSeconds(new Date().getTime() - dtLastConnect.getTime()) : awsKeepAliveSeconds + 1;
    }

    protected long getRemainingConnectionLifetime() {
        return awsKeepAliveSeconds - DEFAULT_CONNECTION_RENEWAL_BEFORE_KEEP_ALIVE_EXPIRATION;
    }

    protected boolean isConnectionAboutToExpire() {
        return getConnectionDuration() > getRemainingConnectionLifetime();
    }

    /**
     * Connects to the websocket-endpoint over an MQTT client. In addition to that it subscribes to all of the topics
     * with respective quality of service.
     *
     * @param context processcontext
     * @return websocket connection client
     * @throws Exception
     */
    protected MqttWebSocketAsyncClient connect(ProcessContext context) {
        AWSCredentials awsCredentials = getCredentialsProvider(context).getCredentials();

        // generate mqtt endpoint-address with authentication details
        String strEndpointAddress = null;
        try {
            strEndpointAddress = AWS4Signer.getAddress(awsRegion, awsEndpoint, awsCredentials);
        } catch (Exception e) {
            getLogger().error("Error while generating AWS endpoint-address caused by " + e.getMessage());
        }

        // extend clientId with random string in order to ensure unique id per connection
        String clientId = awsClientId + RandomStringUtils.random(12, true, false);

        final MqttConnectOptions options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setKeepAliveInterval(0);
        getLogger().info("Connecting to AWS: " + awsEndpoint + " with " + clientId);

        // set up mqtt-Client with endpoint-address
        MqttWebSocketAsyncClient _mqttClient = null;
        try {
            _mqttClient = new MqttWebSocketAsyncClient(strEndpointAddress, clientId, getLogger());
            // start connecting and wait for completion
            _mqttClient.connect(options).waitForCompletion();
        } catch (MqttException e) {
            getLogger().error("Error while connecting to AWS websocket-endpoint caused by " + e.getMessage());
        }
        dtLastConnect = new Date();
        getLogger().info("Connected to AWS.");

        return _mqttClient;
    }
}
