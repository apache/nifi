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

package org.apache.nifi.processors.mqtt;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mqtt.common.AbstractMQTTProcessor;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"publish", "MQTT", "IOT"})
@CapabilityDescription("Publishes a message to an MQTT topic")
@SeeAlso({ConsumeMQTT.class})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PublishMQTT extends AbstractMQTTProcessor implements MqttCallback {

    public static final PropertyDescriptor PROP_TOPIC = new PropertyDescriptor.Builder()
            .name("Topic")
            .description("The topic to publish the message to.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_QOS = new PropertyDescriptor.Builder()
            .name("Quality of Service(QoS)")
            .description("The Quality of Service(QoS) to send the message with. Accepts three values '0', '1' and '2'; '0' for 'at most once', '1' for 'at least once', '2' for 'exactly once'. " +
                    "Expression language is allowed in order to support publishing messages with different QoS but the end value of the property must be either '0', '1' or '2'. ")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(QOS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_RETAIN = new PropertyDescriptor.Builder()
            .name("Retain Message")
            .description("Whether or not the retain flag should be set on the MQTT message.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(RETAIN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are transferred to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are transferred to this relationship.")
            .build();

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;

    static {
        final List<PropertyDescriptor> innerDescriptorsList = getAbstractPropertyDescriptors();
        innerDescriptorsList.add(PROP_TOPIC);
        innerDescriptorsList.add(PROP_QOS);
        innerDescriptorsList.add(PROP_RETAIN);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);

        final Set<Relationship> innerRelationshipsSet = new HashSet<>();
        innerRelationshipsSet.add(REL_SUCCESS);
        innerRelationshipsSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(innerRelationshipsSet);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        synchronized (this) {
            super.onStopped();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();
        if (flowfile == null) {
            return;
        }

        if (!isConnected()){
            synchronized (this) {
                if (!isConnected()) {
                    initializeClient(context);
                }
            }
        }

        // get the MQTT topic
        String topic = context.getProperty(PROP_TOPIC).evaluateAttributeExpressions(flowfile).getValue();

        if (topic == null || topic.isEmpty()) {
            logger.warn("Evaluation of the topic property returned null or evaluated to be empty, routing to failure");
            session.transfer(flowfile, REL_FAILURE);
            return;
        }

        // do the read
        final byte[] messageContent = new byte[(int) flowfile.getSize()];
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });

        int qos = context.getProperty(PROP_QOS).evaluateAttributeExpressions(flowfile).asInteger();
        final MqttMessage mqttMessage = new MqttMessage(messageContent);
        mqttMessage.setQos(qos);
        mqttMessage.setPayload(messageContent);
        mqttMessage.setRetained(context.getProperty(PROP_RETAIN).evaluateAttributeExpressions(flowfile).asBoolean());

        try {
            final StopWatch stopWatch = new StopWatch(true);
            /*
             * Underlying method waits for the message to publish (according to set QoS), so it executes synchronously:
             *     MqttClient.java:361 aClient.publish(topic, message, null, null).waitForCompletion(getTimeToWait());
             */
            mqttClient.publish(topic, mqttMessage);

            session.getProvenanceReporter().send(flowfile, broker, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowfile, REL_SUCCESS);
        } catch(MqttException me) {
            logger.error("Failed to publish message.", me);
            session.transfer(flowfile, REL_FAILURE);
        }
    }

    private void initializeClient(ProcessContext context) {
        // NOTE: This method is called when isConnected returns false which can happen when the client is null, or when it is
        // non-null but not connected, so we need to handle each case and only create a new client when it is null
        try {
            if (mqttClient == null) {
                logger.debug("Creating client");
                mqttClient = createMqttClient(broker, clientID, persistence);
                mqttClient.setCallback(this);
            }

            if (!mqttClient.isConnected()) {
                logger.debug("Connecting client");
                mqttClient.connect(connOpts);
            }
        } catch (MqttException e) {
            logger.error("Connection to {} lost (or was never connected) and connection failed. Yielding processor", new Object[]{broker}, e);
            context.yield();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.error("Connection to {} lost due to: {}", new Object[]{broker, cause.getMessage()}, cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        logger.error("Message arrived to a PublishMQTT processor { topic:'" + topic +"; payload:"+ Arrays.toString(message.getPayload())+"}");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Client.publish waits for message to be delivered so this token will always have a null message and is useless in this application.
        logger.trace("Received 'delivery complete' message from broker for:" + token.toString());
    }

}
