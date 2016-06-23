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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map;

@Tags({"Amazon", "AWS", "IOT", "MQTT", "Websockets", "Put", "Publish", "Send"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Publishes messages to MQTT-topic(s) of AWS IoT. The processor keeps open a WebSocket connection and will automatically renew the " +
        "connection to overcome Amazon's service limit on maximum connection duration. Most of the " +
        "configuration can be overridden by values coming in as message attributes. This applies for " +
        "the topic (corresponding message attribute is \"aws.iot.mqtt.topic.override\"), the qos-level " +
        "(\"aws.iot.mqtt.qos.override\") and the retention (\"aws.iot.mqtt.retained.override\")")
@SeeAlso({ PutAWSIoTShadow.class })
@ReadsAttributes({
        @ReadsAttribute(attribute = "aws.iot.mqtt.topic.override", description = "Overrides the processor configuration for topic."),
        @ReadsAttribute(attribute = "aws.iot.mqtt.qos.override", description = "Overrides the processor configuration for quality of service."),
        @ReadsAttribute(attribute = "aws.iot.mqtt.retained.override", description = "Overrides the processor configuration for retaining a published state in the AWS shadow.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "aws.iot.mqtt.exception", description = "Error details")
})
public class PublishAWSIoTMqtt extends AbstractAWSIoTProcessor {
    private final static String PROP_NAME_RETAINED = "aws.iot.mqtt.retained";
    private final static String ATTR_NAME_TOPIC = PROP_NAME_TOPIC + ".override";
    private final static String ATTR_NAME_QOS = PROP_NAME_QOS + ".override";
    private final static String ATTR_NAME_RETAINED = PROP_NAME_RETAINED + ".override";
    private final static String ATTR_NAME_EXCEPTION = "aws.iot.mqtt.exception";
    private final static Boolean PROP_DEFAULT_RETAINED = false;
    private Boolean shouldRetain;

    public static final PropertyDescriptor PROP_RETAINED = new PropertyDescriptor
            .Builder().name(PROP_NAME_RETAINED)
            .description("For messages being published, a true setting indicates that the MQTT server " +
                    "should retain a copy of the message. The message will then be transmitted to new " +
                    "subscribers to a topic that matches the message topic. For subscribers registering " +
                    "a new subscription, the flag being true indicates that the received message is not " +
                    "a new one, but one that has been retained by the MQTT server.")
            .required(true)
            .defaultValue(PROP_DEFAULT_RETAINED.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(
                    PROP_QOS,
                    PROP_TOPIC,
                    PROP_RETAINED,
                    PROP_ENDPOINT,
                    PROP_KEEPALIVE,
                    PROP_CLIENT,
                    AWS_CREDENTIALS_PROVIDER_SERVICE,
                    TIMEOUT,
                    REGION));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        shouldRetain = context.getProperty(PROP_RETAINED).isSet() ? context.getProperty(PROP_RETAINED).asBoolean() : PROP_DEFAULT_RETAINED;
        init(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        // check if MQTT-connection is about to expire
        if (isConnectionAboutToExpire()) {
            // renew connection
            mqttClient = connect(context);
        }
        // get flowfile
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        Map<String, String> attributes = flowFile.getAttributes();
        // if provided override MQTT configuration with values from the corresponding message attributes
        String topic = attributes.containsKey(ATTR_NAME_TOPIC) ? attributes.get(ATTR_NAME_TOPIC) : awsTopic;
        Integer qos = attributes.containsKey(ATTR_NAME_QOS) ? Integer.parseInt(attributes.get(ATTR_NAME_QOS)) : awsQos;
        Boolean retained = attributes.containsKey(ATTR_NAME_RETAINED) ? Boolean.parseBoolean(attributes.get(ATTR_NAME_RETAINED)) : shouldRetain;
        // get message content
        final ByteArrayOutputStream fileContentStream = new ByteArrayOutputStream();
        session.exportTo(flowFile, fileContentStream);

        try {
            // publish messages to mqtt-topic(s)
            mqttClient.publish(topic, fileContentStream.toByteArray(), qos, retained).waitForCompletion();
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, awsEndpoint + "(" + awsClientId + ")");
        } catch (MqttException e) {
            getLogger().error("Error while publishing to topics with client " + mqttClient.getClientId() + " caused by " + e.getMessage());
            context.yield();
        }
    }
}
