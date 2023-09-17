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
package org.apache.nifi.jms.processors;

import org.apache.nifi.jms.processors.MessageBodyToBytesConverter.MessageConversionException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.jms.support.JmsUtils;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Generic consumer of messages from JMS compliant messaging system.
 */
class JMSConsumer extends JMSWorker {

    private final static int MAX_MESSAGES_PER_FLOW_FILE = 10000;

    JMSConsumer(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ComponentLog logger) {
        super(connectionFactory, jmsTemplate, logger);
        logger.debug("Created Message Consumer for '{}'", jmsTemplate);
    }


    private MessageConsumer createMessageConsumer(final Session session, final String destinationName, final boolean durable, final boolean shared, final String subscriptionName,
                                                  final String messageSelector) throws JMSException {
        final boolean isPubSub = JMSConsumer.this.jmsTemplate.isPubSubDomain();
        final Destination destination = JMSConsumer.this.jmsTemplate.getDestinationResolver().resolveDestinationName(session, destinationName, isPubSub);

        if (isPubSub) {
            if (shared) {
                try {
                    if (durable) {
                        return session.createSharedDurableConsumer((Topic) destination, subscriptionName, messageSelector);
                    } else {
                        return session.createSharedConsumer((Topic) destination, subscriptionName, messageSelector);
                    }
                } catch (AbstractMethodError e) {
                    throw new ProcessException("Failed to create a shared consumer. Make sure the target broker is JMS 2.0 compliant.", e);
                }
            } else {
                if (durable) {
                    return session.createDurableConsumer((Topic) destination, subscriptionName, messageSelector, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                } else {
                    return session.createConsumer(destination, messageSelector, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                }
            }
        } else {
            return session.createConsumer(destination, messageSelector, JMSConsumer.this.jmsTemplate.isPubSubDomain());
        }
    }

    /**
     * Receives a message from the broker. It is the consumerCallback's responsibility to acknowledge the received message.
     */
    public void consumeSingleMessage(final String destinationName, String errorQueueName, final boolean durable, final boolean shared, final String subscriptionName, final String messageSelector,
                                     final String charset, final Consumer<JMSResponse> singleMessageConsumer) {
        doWithJmsTemplate(destinationName, durable, shared, subscriptionName, messageSelector, (session, messageConsumer) -> {
            final JMSResponse response = receiveMessage(session, messageConsumer, charset, errorQueueName);
            if (response != null) {
                // Provide the JMSResponse to the processor to handle. It is the responsibility of the
                // processor to handle acknowledgment of the message (if Client Acknowledge), and it is
                // the responsibility of the processor to handle closing the Message Consumer.
                // Both of these actions can be handled by calling the acknowledge() or reject() methods of
                // the JMSResponse.
                singleMessageConsumer.accept(response);
            }
        });
    }

    /**
     * Receives a list of messages from the broker. It is the consumerCallback's responsibility to acknowledge the received message.
     */
    public void consumeMessageSet(final String destinationName, String errorQueueName, final boolean durable, final boolean shared, final String subscriptionName, final String messageSelector,
                        final String charset, final Consumer<List<JMSResponse>> messageSetConsumer) {
        doWithJmsTemplate(destinationName, durable, shared, subscriptionName, messageSelector, new MessageReceiver() {
            @Override
            public void consume(Session session, MessageConsumer messageConsumer) throws JMSException {
                final List<JMSResponse> jmsResponses = new ArrayList<>();
                int batchCounter = 0;

                JMSResponse response;
                while ((response = receiveMessage(session, messageConsumer, charset, errorQueueName)) != null && batchCounter < MAX_MESSAGES_PER_FLOW_FILE) {
                    response.setBatchOrder(batchCounter);
                    jmsResponses.add(response);
                    batchCounter++;
                }

                if (!jmsResponses.isEmpty()) {
                    // Provide the JMSResponse to the processor to handle. It is the responsibility of the
                    // processor to handle acknowledgment of the message (if Client Acknowledge), and it is
                    // the responsibility of the processor to handle closing the Message Consumer.
                    // Both of these actions can be handled by calling the acknowledge() or reject() methods of
                    // the JMSResponse.
                    messageSetConsumer.accept(jmsResponses);
                }
            }
        });
    }

    private void doWithJmsTemplate(String destinationName, boolean durable, boolean shared, String subscriptionName, String messageSelector, MessageReceiver messageReceiver) {
        this.jmsTemplate.execute(new SessionCallback<Void>() {
            @Override
            public Void doInJms(final Session session) throws JMSException {

                final MessageConsumer messageConsumer = createMessageConsumer(session, destinationName, durable, shared, subscriptionName, messageSelector);
                try {
                    messageReceiver.consume(session, messageConsumer);
                } catch (Exception e) {
                    // We need to call recover to ensure that in the event of
                    // abrupt end or exception the current session will stop message
                    // delivery and restart with the oldest unacknowledged message
                    try {
                        session.recover();
                    } catch (Exception e1) {
                        // likely the session is closed...need to catch this so that the root cause of failure is propagated
                        processLog.debug("Failed to recover JMS session while handling initial error. The recover error is: ", e1);
                    }

                    JmsUtils.closeMessageConsumer(messageConsumer);
                    throw e;
                }

                return null;
            }
        }, true);
    }

    private JMSResponse receiveMessage(Session session, MessageConsumer msgConsumer, String charset, String errorQueueName) throws JMSException {
        final Message message = msgConsumer.receive(JMSConsumer.this.jmsTemplate.getReceiveTimeout());

        // If there is no message, there's nothing for us to do. We can simply close the consumer and return.
        if (message == null) {
            JmsUtils.closeMessageConsumer(msgConsumer);
            return null;
        }

        String messageType;
        byte[] messageBody;

        try {
            if (message instanceof TextMessage) {
                messageType = TextMessage.class.getSimpleName();
                messageBody = MessageBodyToBytesConverter.toBytes((TextMessage) message, Charset.forName(charset));
            } else if (message instanceof BytesMessage) {
                messageType = BytesMessage.class.getSimpleName();
                messageBody = MessageBodyToBytesConverter.toBytes((BytesMessage) message);
            } else if (message instanceof ObjectMessage) {
                messageType = ObjectMessage.class.getSimpleName();
                messageBody = MessageBodyToBytesConverter.toBytes((ObjectMessage) message);
            } else if (message instanceof StreamMessage) {
                messageType = StreamMessage.class.getSimpleName();
                messageBody = MessageBodyToBytesConverter.toBytes((StreamMessage) message);
            } else if (message instanceof MapMessage) {
                messageType = MapMessage.class.getSimpleName();
                messageBody = MessageBodyToBytesConverter.toBytes((MapMessage) message);
            } else {
                acknowledge(message, session);

                if (errorQueueName != null) {
                    processLog.error("Received unsupported JMS Message type [{}]; rerouting message to error queue [{}].", message, errorQueueName);
                    jmsTemplate.send(errorQueueName, __ -> message);
                } else {
                    processLog.error("Received unsupported JMS Message type [{}]; will skip this message.", message);
                }

                return null;
            }
        } catch (final MessageConversionException mce) {
            processLog.error("Received a JMS Message [{}] but failed to obtain the content of the message; will acknowledge this message without creating a FlowFile for it.", message, mce);
            acknowledge(message, session);

            if (errorQueueName != null) {
                jmsTemplate.send(errorQueueName, __ -> message);
            }

            return null;
        }

        final Map<String, String> messageHeaders = extractMessageHeaders(message);
        final Map<String, String> messageProperties = extractMessageProperties(message);

        return new JMSResponse(message, session.getAcknowledgeMode(), messageType, messageBody, messageHeaders, messageProperties, msgConsumer);
    }

    private void acknowledge(final Message message, final Session session) throws JMSException {
        if (message != null && session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
            message.acknowledge();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> extractMessageProperties(final Message message) {
        final Map<String, String> properties = new HashMap<>();
        try {
            final Enumeration<String> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement();
                properties.put(propertyName, String.valueOf(message.getObjectProperty(propertyName)));
            }
        } catch (JMSException e) {
            this.processLog.warn("Failed to extract message properties", e);
        }
        return properties;
    }


    private Map<String, String> extractMessageHeaders(final Message message) throws JMSException {
        final Map<String, String> messageHeaders = new HashMap<>();

        messageHeaders.put(JmsHeaders.DELIVERY_MODE, String.valueOf(message.getJMSDeliveryMode()));
        messageHeaders.put(JmsHeaders.EXPIRATION, String.valueOf(message.getJMSExpiration()));
        messageHeaders.put(JmsHeaders.PRIORITY, String.valueOf(message.getJMSPriority()));
        messageHeaders.put(JmsHeaders.REDELIVERED, String.valueOf(message.getJMSRedelivered()));
        messageHeaders.put(JmsHeaders.TIMESTAMP, String.valueOf(message.getJMSTimestamp()));
        messageHeaders.put(JmsHeaders.CORRELATION_ID, message.getJMSCorrelationID());
        messageHeaders.put(JmsHeaders.MESSAGE_ID, message.getJMSMessageID());
        messageHeaders.put(JmsHeaders.TYPE, message.getJMSType());

        String replyToDestinationName = this.retrieveDestinationName(message.getJMSReplyTo(), JmsHeaders.REPLY_TO);
        if (replyToDestinationName != null) {
            messageHeaders.put(JmsHeaders.REPLY_TO, replyToDestinationName);
        }

        String destinationName = this.retrieveDestinationName(message.getJMSDestination(), JmsHeaders.DESTINATION);
        if (destinationName != null) {
            messageHeaders.put(JmsHeaders.DESTINATION, destinationName);
        }

        return messageHeaders;
    }


    private String retrieveDestinationName(Destination destination, String headerName) {
        String destinationName = null;
        if (destination != null) {
            try {
                destinationName = (destination instanceof Queue) ? ((Queue) destination).getQueueName()
                        : ((Topic) destination).getTopicName();
            } catch (JMSException e) {
                this.processLog.warn("Failed to retrieve Destination name for '" + headerName + "' header", e);
            }
        }
        return destinationName;
    }


    static class JMSResponse {
        private final Message message;
        private final int acknowledgementMode;
        private final byte[] messageBody;
        private final String messageType;
        private final Map<String, String> messageHeaders;
        private final Map<String, String> messageProperties;
        private final MessageConsumer messageConsumer;
        private Integer batchOrder;

        JMSResponse(final Message message, final int acknowledgementMode, final String messageType, final byte[] messageBody, final Map<String, String> messageHeaders,
                    final Map<String, String> messageProperties, final MessageConsumer msgConsumer) {
            this.message = message;
            this.acknowledgementMode = acknowledgementMode;
            this.messageType = messageType;
            this.messageBody = messageBody;
            this.messageHeaders = Collections.unmodifiableMap(messageHeaders);
            this.messageProperties = Collections.unmodifiableMap(messageProperties);
            this.messageConsumer = msgConsumer;
        }

        public String getMessageType() {
            return messageType;
        }

        public byte[] getMessageBody() {
            return this.messageBody;
        }

        public Map<String, String> getMessageHeaders() {
            return this.messageHeaders;
        }

        public Map<String, String> getMessageProperties() {
            return messageProperties;
        }

        public void acknowledge() throws JMSException {
            try {
                if (acknowledgementMode == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                }
            } finally {
                JmsUtils.closeMessageConsumer(messageConsumer);
            }
        }

        public void reject() {
            JmsUtils.closeMessageConsumer(messageConsumer);
        }

        public Integer getBatchOrder() {
            return batchOrder;
        }

        public void setBatchOrder(Integer batchOrder) {
            this.batchOrder = batchOrder;
        }
    }

    interface MessageReceiver {
        void consume(Session session, MessageConsumer messageConsumer) throws JMSException;
    }

}
