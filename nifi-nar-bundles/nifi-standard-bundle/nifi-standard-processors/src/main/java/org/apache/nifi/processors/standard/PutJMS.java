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
package org.apache.nifi.processors.standard;

import static org.apache.nifi.processors.standard.util.JmsFactory.ATTRIBUTE_PREFIX;
import static org.apache.nifi.processors.standard.util.JmsFactory.ATTRIBUTE_TYPE_SUFFIX;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_BOOLEAN;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_BYTE;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_DOUBLE;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_FLOAT;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_INTEGER;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_LONG;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_OBJECT;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_SHORT;
import static org.apache.nifi.processors.standard.util.JmsFactory.PROP_TYPE_STRING;
import static org.apache.nifi.processors.standard.util.JmsProperties.ATTRIBUTES_TO_JMS_PROPS;
import static org.apache.nifi.processors.standard.util.JmsProperties.BATCH_SIZE;
import static org.apache.nifi.processors.standard.util.JmsProperties.CLIENT_ID_PREFIX;
import static org.apache.nifi.processors.standard.util.JmsProperties.DESTINATION_NAME;
import static org.apache.nifi.processors.standard.util.JmsProperties.DESTINATION_TYPE;
import static org.apache.nifi.processors.standard.util.JmsProperties.JMS_PROVIDER;
import static org.apache.nifi.processors.standard.util.JmsProperties.MAX_BUFFER_SIZE;
import static org.apache.nifi.processors.standard.util.JmsProperties.MESSAGE_PRIORITY;
import static org.apache.nifi.processors.standard.util.JmsProperties.MESSAGE_TTL;
import static org.apache.nifi.processors.standard.util.JmsProperties.MESSAGE_TYPE;
import static org.apache.nifi.processors.standard.util.JmsProperties.MSG_TYPE_BYTE;
import static org.apache.nifi.processors.standard.util.JmsProperties.MSG_TYPE_EMPTY;
import static org.apache.nifi.processors.standard.util.JmsProperties.MSG_TYPE_MAP;
import static org.apache.nifi.processors.standard.util.JmsProperties.MSG_TYPE_STREAM;
import static org.apache.nifi.processors.standard.util.JmsProperties.MSG_TYPE_TEXT;
import static org.apache.nifi.processors.standard.util.JmsProperties.PASSWORD;
import static org.apache.nifi.processors.standard.util.JmsProperties.REPLY_TO_QUEUE;
import static org.apache.nifi.processors.standard.util.JmsProperties.TIMEOUT;
import static org.apache.nifi.processors.standard.util.JmsProperties.URL;
import static org.apache.nifi.processors.standard.util.JmsProperties.USERNAME;
import static org.apache.nifi.processors.standard.util.JmsProperties.SSL_CONTEXT_SERVICE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.standard.util.JmsFactory;
import org.apache.nifi.processors.standard.util.WrappedMessageProducer;
import org.apache.nifi.stream.io.StreamUtils;

@Deprecated
@DeprecationNotice(classNames = {"org.apache.nifi.jms.processors.PublishJMS"}, reason = "This processor is deprecated and may be removed in future releases.")
@Tags({"jms", "send", "put"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates a JMS Message from the contents of a FlowFile and sends the message to a ActiveMQ JMS Server.")
@SeeAlso({GetJMSQueue.class, GetJMSTopic.class, })
public class PutJMS extends AbstractProcessor {

    public static final Charset UTF8 = Charset.forName("UTF-8");
    public static final int DEFAULT_MESSAGE_PRIORITY = 4;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the JMS destination are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be routed to the JMS destination are routed to this relationship")
            .build();

    private final Queue<WrappedMessageProducer> producerQueue = new LinkedBlockingQueue<>();
    private final List<PropertyDescriptor> properties;
    private final Set<Relationship> relationships;

    public PutJMS() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(JMS_PROVIDER);
        descriptors.add(URL);
        descriptors.add(DESTINATION_NAME);
        descriptors.add(DESTINATION_TYPE);
        descriptors.add(TIMEOUT);
        descriptors.add(BATCH_SIZE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(MESSAGE_TYPE);
        descriptors.add(MESSAGE_PRIORITY);
        descriptors.add(REPLY_TO_QUEUE);
        descriptors.add(MAX_BUFFER_SIZE);
        descriptors.add(MESSAGE_TTL);
        descriptors.add(ATTRIBUTES_TO_JMS_PROPS);
        descriptors.add(CLIENT_ID_PREFIX);
        this.properties = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnStopped
    public void cleanupResources() {
        WrappedMessageProducer wrappedProducer = producerQueue.poll();
        while (wrappedProducer != null) {
            wrappedProducer.close(getLogger());
            wrappedProducer = producerQueue.poll();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger().intValue());
        if (flowFiles.isEmpty()) {
            return;
        }

        WrappedMessageProducer wrappedProducer = producerQueue.poll();
        if (wrappedProducer == null) {
            try {
                wrappedProducer = JmsFactory.createMessageProducer(context, true);
                logger.info("Connected to JMS server {}", new Object[]{context.getProperty(URL).getValue()});
            } catch (final JMSException e) {
                logger.error("Failed to connect to JMS Server due to {}", new Object[]{e});
                session.transfer(flowFiles, REL_FAILURE);
                context.yield();
                return;
            }
        }

        final Session jmsSession = wrappedProducer.getSession();
        final MessageProducer producer = wrappedProducer.getProducer();

        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        try {
            final Set<FlowFile> successfulFlowFiles = new HashSet<>();

            for (FlowFile flowFile : flowFiles) {
                if (flowFile.getSize() > maxBufferSize) {
                    session.transfer(flowFile, REL_FAILURE);
                    logger.warn("Routing {} to failure because its size exceeds the configured max", new Object[]{flowFile});
                    continue;
                }

                // Read the contents of the FlowFile into a byte array
                final byte[] messageContent = new byte[(int) flowFile.getSize()];
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        StreamUtils.fillBuffer(in, messageContent, true);
                    }
                });

                final Long ttl = context.getProperty(MESSAGE_TTL).asTimePeriod(TimeUnit.MILLISECONDS);

                final String replyToQueueName = context.getProperty(REPLY_TO_QUEUE).evaluateAttributeExpressions(flowFile).getValue();
                final Destination replyToQueue = replyToQueueName == null ? null : JmsFactory.createQueue(context, replyToQueueName);

                int priority = DEFAULT_MESSAGE_PRIORITY;
                try {
                    final Integer priorityInt = context.getProperty(MESSAGE_PRIORITY).evaluateAttributeExpressions(flowFile).asInteger();
                    priority = priorityInt == null ? priority : priorityInt;
                } catch (final NumberFormatException e) {
                    logger.warn("Invalid value for JMS Message Priority: {}; defaulting to priority of {}",
                            new Object[]{context.getProperty(MESSAGE_PRIORITY).evaluateAttributeExpressions(flowFile).getValue(), DEFAULT_MESSAGE_PRIORITY});
                }

                try {
                    final Message message = createMessage(jmsSession, context, messageContent, flowFile, replyToQueue, priority);
                    if (ttl == null) {
                        producer.setTimeToLive(0L);
                    } else {
                        producer.setTimeToLive(ttl);
                    }
                    producer.send(message);
                } catch (final JMSException e) {
                    logger.error("Failed to send {} to JMS Server due to {}", new Object[]{flowFile, e});
                    session.transfer(flowFiles, REL_FAILURE);
                    context.yield();

                    try {
                        jmsSession.rollback();
                    } catch (final JMSException jmse) {
                        logger.warn("Unable to roll back JMS Session due to {}", new Object[]{jmse});
                    }

                    wrappedProducer.close(logger);
                    return;
                }

                successfulFlowFiles.add(flowFile);
                session.getProvenanceReporter().send(flowFile, context.getProperty(URL).getValue());
            }

            try {
                jmsSession.commit();

                session.transfer(successfulFlowFiles, REL_SUCCESS);
                final String flowFileDescription = successfulFlowFiles.size() > 10 ? successfulFlowFiles.size() + " FlowFiles" : successfulFlowFiles.toString();
                logger.info("Sent {} to JMS Server and transferred to 'success'", new Object[]{flowFileDescription});
            } catch (JMSException e) {
                logger.error("Failed to commit JMS Session due to {} and transferred to 'failure'", new Object[]{e});
                session.transfer(flowFiles, REL_FAILURE);
                context.yield();
                wrappedProducer.close(logger);
            }
        } finally {
            if (!wrappedProducer.isClosed()) {
                producerQueue.offer(wrappedProducer);
            }
        }
    }

    private Message createMessage(final Session jmsSession, final ProcessContext context, final byte[] messageContent,
            final FlowFile flowFile, final Destination replyToQueue, final Integer priority) throws JMSException {
        final Message message;

        switch (context.getProperty(MESSAGE_TYPE).getValue()) {
            case MSG_TYPE_EMPTY: {
                message = jmsSession.createTextMessage("");
                break;
            }
            case MSG_TYPE_STREAM: {
                final StreamMessage streamMessage = jmsSession.createStreamMessage();
                streamMessage.writeBytes(messageContent);
                message = streamMessage;
                break;
            }
            case MSG_TYPE_TEXT: {
                message = jmsSession.createTextMessage(new String(messageContent, UTF8));
                break;
            }
            case MSG_TYPE_MAP: {
                message = jmsSession.createMapMessage();
                break;
            }
            case MSG_TYPE_BYTE:
            default: {
                final BytesMessage bytesMessage = jmsSession.createBytesMessage();
                bytesMessage.writeBytes(messageContent);
                message = bytesMessage;
            }
        }

        message.setJMSTimestamp(System.currentTimeMillis());

        if (replyToQueue != null) {
            message.setJMSReplyTo(replyToQueue);
        }

        if (priority != null) {
            message.setJMSPriority(priority);
        }

        if (context.getProperty(ATTRIBUTES_TO_JMS_PROPS).asBoolean()) {
            copyAttributesToJmsProps(flowFile, message);
        }

        return message;
    }

    /**
     * Iterates through all of the flow file's metadata and for any metadata key that starts with <code>jms.</code>, the value for the corresponding key is written to the JMS message as a property.
     * The name of this property is equal to the key of the flow file's metadata minus the <code>jms.</code>. For example, if the flowFile has a metadata entry:
     * <br /><br />
     * <code>jms.count</code> = <code>8</code>
     * <br /><br />
     * then the JMS message will have a String property added to it with the property name <code>count</code> and value <code>8</code>.
     *
     * If the flow file also has a metadata key with the name <code>jms.count.type</code>, then the value of that metadata entry will determine the JMS property type to use for the value. For example,
     * if the flow file has the following properties:
     * <br /><br />
     * <code>jms.count</code> = <code>8</code><br />
     * <code>jms.count.type</code> = <code>integer</code>
     * <br /><br />
     * Then <code>message</code> will have an INTEGER property added with the value 8.
     * <br /><br/>
     * If the type is not valid for the given value (e.g., <code>jms.count.type</code> = <code>integer</code> and <code>jms.count</code> = <code>hello</code>, then this JMS property will not be added
     * to <code>message</code>.
     *
     * @param flowFile The flow file whose metadata should be examined for JMS properties.
     * @param message The JMS message to which we want to add properties.
     * @throws JMSException ex
     */
    private void copyAttributesToJmsProps(final FlowFile flowFile, final Message message) throws JMSException {
        final ComponentLog logger = getLogger();

        final Map<String, String> attributes = flowFile.getAttributes();
        for (final Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();

            if (key.toLowerCase().startsWith(ATTRIBUTE_PREFIX.toLowerCase()) && !key.toLowerCase().endsWith(ATTRIBUTE_TYPE_SUFFIX.toLowerCase())) {

                final String jmsPropName = key.substring(ATTRIBUTE_PREFIX.length());
                final String type = attributes.get(key + ATTRIBUTE_TYPE_SUFFIX);

                try {
                    if (type == null || type.equalsIgnoreCase(PROP_TYPE_STRING)) {
                        message.setStringProperty(jmsPropName, value);
                    } else if (type.equalsIgnoreCase(PROP_TYPE_INTEGER)) {
                        message.setIntProperty(jmsPropName, Integer.parseInt(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_BOOLEAN)) {
                        message.setBooleanProperty(jmsPropName, Boolean.parseBoolean(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_SHORT)) {
                        message.setShortProperty(jmsPropName, Short.parseShort(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_LONG)) {
                        message.setLongProperty(jmsPropName, Long.parseLong(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_BYTE)) {
                        message.setByteProperty(jmsPropName, Byte.parseByte(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_DOUBLE)) {
                        message.setDoubleProperty(jmsPropName, Double.parseDouble(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_FLOAT)) {
                        message.setFloatProperty(jmsPropName, Float.parseFloat(value));
                    } else if (type.equalsIgnoreCase(PROP_TYPE_OBJECT)) {
                        message.setObjectProperty(jmsPropName, value);
                    } else {
                        logger.warn("Attribute key '{}' for {} has value '{}', but expected one of: integer, string, object, byte, double, float, long, short, boolean; not adding this property",
                                new Object[]{key, flowFile, value});
                    }
                } catch (NumberFormatException e) {
                    logger.warn("Attribute key '{}' for {} has value '{}', but attribute key '{}' has value '{}'. Not adding this JMS property",
                            new Object[]{key, flowFile, value, key + ATTRIBUTE_TYPE_SUFFIX, PROP_TYPE_INTEGER});
                }
            }
        }
    }
}
