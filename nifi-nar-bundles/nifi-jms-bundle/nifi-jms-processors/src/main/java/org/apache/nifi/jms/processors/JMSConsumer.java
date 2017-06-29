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

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.jms.support.JmsUtils;

/**
 * Generic consumer of messages from JMS compliant messaging system.
 */
final class JMSConsumer extends JMSWorker {

    /**
     * Creates an instance of this consumer
     *
     * @param jmsTemplate
     *            instance of {@link JmsTemplate}
     * @param processLog
     *            instance of {@link ComponentLog}
     */
    JMSConsumer(JmsTemplate jmsTemplate, ComponentLog processLog) {
        super(jmsTemplate, processLog);
        if (this.processLog.isInfoEnabled()) {
            this.processLog.info("Created Message Consumer for '" + jmsTemplate.toString() + "'.");
        }
    }


    /**
     *
     */
    public void consume(final String destinationName, final boolean durable, final boolean shared, final String subscriberName, final ConsumerCallback consumerCallback) {
        this.jmsTemplate.execute(new SessionCallback<Void>() {
            @Override
            public Void doInJms(Session session) throws JMSException {
                /*
                 * We need to call recover to ensure that in in the event of
                 * abrupt end or exception the current session will stop message
                 * delivery and restarts with the oldest unacknowledged message
                 */
                session.recover();
                boolean isPubSub = JMSConsumer.this.jmsTemplate.isPubSubDomain();
                Destination destination = JMSConsumer.this.jmsTemplate.getDestinationResolver().resolveDestinationName(
                        session, destinationName, isPubSub);
                MessageConsumer msgConsumer;
                if (isPubSub) {
                    if (shared) {
                        try {
                            if (durable) {
                                msgConsumer = session.createSharedDurableConsumer((Topic)destination, subscriberName);
                            } else {
                                msgConsumer = session.createSharedConsumer((Topic)destination, subscriberName);
                            }
                        } catch (AbstractMethodError e) {
                            throw new ProcessException("Failed to create a shared consumer. Make sure the target broker is JMS 2.0 compliant.", e);
                        }
                    } else {
                        if (durable) {
                            msgConsumer = session.createDurableConsumer((Topic)destination, subscriberName, null, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                        } else {
                            msgConsumer = session.createConsumer((Topic)destination, null, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                        }
                    }
                } else {
                    msgConsumer = session.createConsumer(destination, null, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                }
                Message message = msgConsumer.receive(JMSConsumer.this.jmsTemplate.getReceiveTimeout());
                JMSResponse response = null;
                try {
                    if (message != null) {
                        byte[] messageBody = null;
                        if (message instanceof TextMessage) {
                            messageBody = MessageBodyToBytesConverter.toBytes((TextMessage) message);
                        } else if (message instanceof BytesMessage) {
                            messageBody = MessageBodyToBytesConverter.toBytes((BytesMessage) message);
                        } else {
                            throw new IllegalStateException("Message type other then TextMessage and BytesMessage are "
                                    + "not supported at the moment");
                        }
                        Map<String, Object> messageHeaders = extractMessageHeaders(message);
                        Map<String, String> messageProperties = extractMessageProperties(message);
                        response = new JMSResponse(messageBody, messageHeaders, messageProperties);
                    }
                    // invoke the processor callback (regardless if it's null,
                    // so the processor can yield) as part of this inJMS call
                    // and ACK message *only* after its successful invocation
                    // and if CLIENT_ACKNOWLEDGE is set.
                    consumerCallback.accept(response);
                    if (message != null && session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                        message.acknowledge();
                    }
                } finally {
                    JmsUtils.closeMessageConsumer(msgConsumer);
                }
                return null;
            }
        }, true);
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> extractMessageProperties(Message message) {
        Map<String, String> properties = new HashMap<>();
        try {
            Enumeration<String> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement();
                properties.put(propertyName, String.valueOf(message.getObjectProperty(propertyName)));
            }
        } catch (JMSException e) {
            this.processLog.warn("Failed to extract message properties", e);
        }
        return properties;
    }

    /**
     *
     *
     */
    private Map<String, Object> extractMessageHeaders(Message message) {
        // even though all values are Strings in current impl, it may change in
        // the future, so keeping it <String, Object>
        Map<String, Object> messageHeaders = new HashMap<>();
        try {
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
        } catch (Exception e) {
            throw new IllegalStateException("Failed to extract JMS Headers", e);
        }
        return messageHeaders;
    }

    /**
     *
     */
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

    /**
     *
     */
    static class JMSResponse {
        private final byte[] messageBody;

        private final Map<String, Object> messageHeaders;

        private final Map<String, String> messageProperties;

        JMSResponse(byte[] messageBody, Map<String, Object> messageHeaders, Map<String, String> messageProperties) {
            this.messageBody = messageBody;
            this.messageHeaders = Collections.unmodifiableMap(messageHeaders);
            this.messageProperties = Collections.unmodifiableMap(messageProperties);
        }

        public byte[] getMessageBody() {
            return this.messageBody;
        }

        public Map<String, Object> getMessageHeaders() {
            return this.messageHeaders;
        }

        public Map<String, String> getMessageProperties() {
            return messageProperties;
        }
    }

    /**
     * Callback to be invoked while executing inJMS call (the call within the
     * live JMS session)
     */
    static interface ConsumerCallback {
        void accept(JMSResponse response);
    }
}
