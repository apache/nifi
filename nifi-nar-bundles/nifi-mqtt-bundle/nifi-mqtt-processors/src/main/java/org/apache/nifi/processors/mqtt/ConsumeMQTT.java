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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mqtt.common.AbstractMQTTProcessor;
import org.apache.nifi.processors.mqtt.common.MQTTQueueMessage;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nifi.processors.mqtt.ConsumeMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_0;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_1;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_2;


@Tags({"subscribe", "MQTT", "IOT", "consume", "listen"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@CapabilityDescription("Subscribes to a topic and receives messages from an MQTT broker")
@SeeAlso({PublishMQTT.class})
@WritesAttributes({
    @WritesAttribute(attribute=BROKER_ATTRIBUTE_KEY, description="MQTT broker that was the message source"),
    @WritesAttribute(attribute=TOPIC_ATTRIBUTE_KEY, description="MQTT topic on which message was received"),
    @WritesAttribute(attribute=QOS_ATTRIBUTE_KEY, description="The quality of service for this message."),
    @WritesAttribute(attribute=IS_DUPLICATE_ATTRIBUTE_KEY, description="Whether or not this message might be a duplicate of one which has already been received."),
    @WritesAttribute(attribute=IS_RETAINED_ATTRIBUTE_KEY, description="Whether or not this message was from a current publisher, or was \"retained\" by the server as the last message published " +
            "on the topic.")})
public class ConsumeMQTT extends AbstractMQTTProcessor  implements MqttCallback {

    public final static String BROKER_ATTRIBUTE_KEY =  "mqtt.broker";
    public final static String TOPIC_ATTRIBUTE_KEY =  "mqtt.topic";
    public final static String QOS_ATTRIBUTE_KEY =  "mqtt.qos";
    public final static String IS_DUPLICATE_ATTRIBUTE_KEY =  "mqtt.isDuplicate";
    public final static String IS_RETAINED_ATTRIBUTE_KEY =  "mqtt.isRetained";

    public static final PropertyDescriptor PROP_TOPIC_FILTER = new PropertyDescriptor.Builder()
            .name("Topic Filter")
            .description("The MQTT topic filter to designate the topics to subscribe to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_QOS = new PropertyDescriptor.Builder()
            .name("Quality of Service(QoS)")
            .description("The Quality of Service(QoS) to receive the message with. Accepts values '0', '1' or '2'; '0' for 'at most once', '1' for 'at least once', '2' for 'exactly once'.")
            .required(true)
            .defaultValue(ALLOWABLE_VALUE_QOS_0.getValue())
            .allowableValues(
                    ALLOWABLE_VALUE_QOS_0,
                    ALLOWABLE_VALUE_QOS_1,
                    ALLOWABLE_VALUE_QOS_2)
            .build();

    public static final PropertyDescriptor PROP_MAX_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Queue Size")
            .description("The MQTT messages are always being sent to subscribers on a topic. If the 'Run Schedule' is significantly behind the rate at which the messages are arriving to this " +
                    "processor then a back up can occur. This property specifies the maximum number of messages this processor will hold in memory at one time.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();


    private volatile long maxQueueSize;

    private volatile int qos;
    private volatile String topicFilter;
    private final AtomicBoolean scheduled = new AtomicBoolean(false);

    private volatile LinkedBlockingQueue<MQTTQueueMessage> mqttQueue;

    public static final Relationship REL_MESSAGE = new Relationship.Builder()
            .name("Message")
            .description("The MQTT message output")
            .build();

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;

    static{
        final List<PropertyDescriptor> innerDescriptorsList = getAbstractPropertyDescriptors();
        innerDescriptorsList.add(PROP_TOPIC_FILTER);
        innerDescriptorsList.add(PROP_QOS);
        innerDescriptorsList.add(PROP_MAX_QUEUE_SIZE);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);

        final Set<Relationship> innerRelationshipsSet = new HashSet<Relationship>();
        innerRelationshipsSet.add(REL_MESSAGE);
        relationships = Collections.unmodifiableSet(innerRelationshipsSet);
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // resize the receive buffer, but preserve data
        if (descriptor == PROP_MAX_QUEUE_SIZE) {
            // it's a mandatory integer, never null
            int newSize = Integer.valueOf(newValue);
            if (mqttQueue != null) {
                int msgPending = mqttQueue.size();
                if (msgPending > newSize) {
                    logger.warn("New receive buffer size ({}) is smaller than the number of messages pending ({}), ignoring resize request. Processor will be invalid.",
                            new Object[]{newSize, msgPending});
                    return;
                }
                LinkedBlockingQueue<MQTTQueueMessage> newBuffer = new LinkedBlockingQueue<>(newSize);
                mqttQueue.drainTo(newBuffer);
                mqttQueue = newBuffer;
            }

        }
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = super.customValidate(context);
        int newSize = context.getProperty(PROP_MAX_QUEUE_SIZE).asInteger();
        if (mqttQueue == null) {
            mqttQueue = new LinkedBlockingQueue<>(context.getProperty(PROP_MAX_QUEUE_SIZE).asInteger());
        }
        int msgPending = mqttQueue.size();
        if (msgPending > newSize) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("ConsumeMQTT Configuration")
                    .explanation(String.format("%s (%d) is smaller than the number of messages pending (%d).",
                            PROP_MAX_QUEUE_SIZE.getDisplayName(), newSize, msgPending))
                    .build());
        }

        return results;
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
        qos = context.getProperty(PROP_QOS).asInteger();
        maxQueueSize = context.getProperty(PROP_MAX_QUEUE_SIZE).asLong();
        topicFilter = context.getProperty(PROP_TOPIC_FILTER).getValue();
        scheduled.set(true);
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        scheduled.set(false);
        synchronized (this) {
            super.onStopped();
        }
    }


    @OnStopped
    public void onStopped(final ProcessContext context) throws IOException {
        if(mqttQueue != null && !mqttQueue.isEmpty() && processSessionFactory != null) {
            logger.info("Finishing processing leftover messages");
            ProcessSession session = processSessionFactory.createSession();
            transferQueue(session);
        } else {
            if (mqttQueue!= null && !mqttQueue.isEmpty()){
                throw new ProcessException("Stopping the processor but there is no ProcessSessionFactory stored and there are messages in the MQTT internal queue. Removing the processor now will " +
                        "clear the queue but will result in DATA LOSS. This is normally due to starting the processor, receiving messages and stopping before the onTrigger happens. The messages " +
                        "in the MQTT internal queue cannot finish processing until until the processor is triggered to run.");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final boolean isScheduled = scheduled.get();
        if (!isConnected() && isScheduled){
            synchronized (this) {
                if (!isConnected()) {
                    initializeClient(context);
                }
            }
        }

        if (mqttQueue.isEmpty()) {
            return;
        }

        transferQueue(session);
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
                mqttClient.subscribe(topicFilter, qos);
            }
        } catch (MqttException e) {
            logger.error("Connection to {} lost (or was never connected) and connection failed. Yielding processor", new Object[]{broker}, e);
            context.yield();
        }
    }

    private void transferQueue(ProcessSession session){
        while (!mqttQueue.isEmpty()) {
            FlowFile messageFlowfile = session.create();
            final MQTTQueueMessage mqttMessage = mqttQueue.peek();

            Map<String, String> attrs = new HashMap<>();
            attrs.put(BROKER_ATTRIBUTE_KEY, broker);
            attrs.put(TOPIC_ATTRIBUTE_KEY, mqttMessage.getTopic());
            attrs.put(QOS_ATTRIBUTE_KEY, String.valueOf(mqttMessage.getQos()));
            attrs.put(IS_DUPLICATE_ATTRIBUTE_KEY, String.valueOf(mqttMessage.isDuplicate()));
            attrs.put(IS_RETAINED_ATTRIBUTE_KEY, String.valueOf(mqttMessage.isRetained()));

            messageFlowfile = session.putAllAttributes(messageFlowfile, attrs);

            messageFlowfile = session.write(messageFlowfile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(mqttMessage.getPayload());
                }
            });

            String transitUri = new StringBuilder(broker).append(mqttMessage.getTopic()).toString();
            session.getProvenanceReporter().receive(messageFlowfile, transitUri);
            session.transfer(messageFlowfile, REL_MESSAGE);
            session.commit();
            if (!mqttQueue.remove(mqttMessage) && logger.isWarnEnabled()) {
                logger.warn(new StringBuilder("FlowFile ")
                        .append(messageFlowfile.getAttribute(CoreAttributes.UUID.key()))
                        .append(" for Mqtt message ")
                        .append(mqttMessage)
                        .append(" had already been removed from queue, possible duplication of flow files")
                        .toString());
            }
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.error("Connection to {} lost due to: {}", new Object[]{broker, cause.getMessage()}, cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (logger.isDebugEnabled()) {
            byte[] payload = message.getPayload();
            String text = new String(payload, "UTF-8");
            if (StringUtils.isAsciiPrintable(text)) {
                logger.debug("Message arrived from topic {}. Payload: {}", new Object[] {topic, text});
            } else {
                logger.debug("Message arrived from topic {}. Binary value of size {}", new Object[] {topic, payload.length});
            }
        }

        if (mqttQueue.size() >= maxQueueSize){
            throw new IllegalStateException("The subscriber queue is full, cannot receive another message until the processor is scheduled to run.");
        } else {
            mqttQueue.add(new MQTTQueueMessage(topic, message));
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.warn("Received MQTT 'delivery complete' message to subscriber: " + token);
    }

}
