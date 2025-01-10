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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.nifi.logging.ComponentLog;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Generic publisher of messages to JMS compliant messaging system.
 */
class JMSPublisher extends JMSWorker {

    JMSPublisher(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ComponentLog processLog) {
        super(connectionFactory, jmsTemplate, processLog);
        processLog.debug("Created Message Publisher for {}", jmsTemplate);
    }

    void publish(String destinationName, byte[] messageBytes) {
        this.publish(destinationName, messageBytes, null);
    }

    void publish(final String destinationName, final byte[] messageBytes, final Map<String, String> flowFileAttributes) {
        this.jmsTemplate.send(destinationName, session -> {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(messageBytes);
            setMessageHeaderAndProperties(session, message, flowFileAttributes);
            return message;
        });
    }

    void publish(String destinationName, String messageText, final Map<String, String> flowFileAttributes) {
        this.jmsTemplate.send(destinationName, session -> {
            TextMessage message = session.createTextMessage(messageText);
            setMessageHeaderAndProperties(session, message, flowFileAttributes);
            return message;
        });
    }

    void setMessageHeaderAndProperties(final Session session, final Message message, final Map<String, String> flowFileAttributes) throws JMSException {
        if (flowFileAttributes != null && !flowFileAttributes.isEmpty()) {

            for (Entry<String, String> entry : flowFileAttributes.entrySet()) {
                try {
                    if (entry.getKey().equals(JmsHeaders.DELIVERY_MODE)) {
                        this.jmsTemplate.setDeliveryMode(Integer.parseInt(entry.getValue()));
                        this.jmsTemplate.setExplicitQosEnabled(true);
                    } else if (entry.getKey().equals(JmsHeaders.EXPIRATION)) {
                        if (NumberUtils.isCreatable(entry.getValue())) { //ignore any non-numeric values
                            long expiration = Long.parseLong(entry.getValue());
                            long ttl = 0L;

                            // if expiration was set to a positive non-zero value, then calculate the ttl
                            // jmsTemplate does not have an expiration field, and can only accept a ttl value
                            // which is then used to set jms_expiration header
                            // ttl is in epoch millis
                            if (expiration > 0) {
                                ttl = expiration - Instant.now().toEpochMilli();
                                if (ttl > 0) {
                                    this.jmsTemplate.setTimeToLive(ttl);
                                    this.jmsTemplate.setExplicitQosEnabled(true);
                                } // else, use default ttl
                            } else if (expiration == 0) { // expiration == 0 means no expiration in jms
                                this.jmsTemplate.setTimeToLive(0); //ttl must be explicitly set to 0 to indicate no expiration
                                this.jmsTemplate.setExplicitQosEnabled(true);
                            } // else, use default ttl
                        }
                    } else if (entry.getKey().equals(JmsHeaders.PRIORITY)) {
                        this.jmsTemplate.setPriority(Integer.parseInt(entry.getValue()));
                        this.jmsTemplate.setExplicitQosEnabled(true);
                    } else if (entry.getKey().equals(JmsHeaders.REDELIVERED)) {
                        message.setJMSRedelivered(Boolean.parseBoolean(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.TIMESTAMP)) {
                        message.setJMSTimestamp(Long.parseLong(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.CORRELATION_ID)) {
                        message.setJMSCorrelationID(entry.getValue());
                    } else if (entry.getKey().equals(JmsHeaders.TYPE)) {
                        message.setJMSType(entry.getValue());
                    } else if (entry.getKey().equals(JmsHeaders.REPLY_TO)) {
                        Destination destination = buildDestination(session, entry.getValue());
                        if (destination != null) {
                            message.setJMSReplyTo(destination);
                        } else {
                            logUnbuildableDestination(entry.getValue(), JmsHeaders.REPLY_TO);
                        }
                    } else if (entry.getKey().equals(JmsHeaders.DESTINATION)) {
                        Destination destination = buildDestination(session, entry.getValue());
                        if (destination != null) {
                            message.setJMSDestination(destination);
                        } else {
                            logUnbuildableDestination(entry.getValue(), JmsHeaders.DESTINATION);
                        }
                    } else {
                        // not a special attribute handled above, so send it as a property using the specified property type
                        String type = flowFileAttributes.getOrDefault(entry.getKey().concat(".type"), "unknown").toLowerCase();
                        propertySetterMap.getOrDefault(type, JmsPropertySetterEnum.STRING)
                                .setProperty(message, entry.getKey(), entry.getValue());
                    }
                } catch (NumberFormatException ne) {
                    this.processLog.warn("Incompatible value for attribute {} [{}] is not a number. Ignoring this attribute.", entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private void logUnbuildableDestination(String destinationName, String headerName) {
        this.processLog.warn("Failed to determine destination type from destination name '{}'. The '{}' header will not be set.", destinationName, headerName);
    }


    private static Destination buildDestination(final Session session, final String destinationName) throws JMSException {
        if (destinationName.toLowerCase().contains("topic")) {
            return session.createTopic(destinationName);
        } else if (destinationName.toLowerCase().contains("queue")) {
            return session.createQueue(destinationName);
        }
        return null;
    }

    /**
     * Implementations of this interface use {@link jakarta.jms.Message} methods to set strongly typed properties.
     */
    public interface JmsPropertySetter {
        void setProperty(final Message message, final String name, final String value) throws JMSException, NumberFormatException;
    }

    public enum JmsPropertySetterEnum implements JmsPropertySetter {
        BOOLEAN( (message, name, value) -> {
            message.setBooleanProperty(name, Boolean.parseBoolean(value));
        } ),
        BYTE( (message, name, value) -> {
            message.setByteProperty(name, Byte.parseByte(value));
        } ),
        SHORT( (message, name, value) -> {
            message.setShortProperty(name, Short.parseShort(value));
        } ),
        INTEGER( (message, name, value) -> {
            message.setIntProperty(name, Integer.parseInt(value));
        } ),
        LONG( (message, name, value) -> {
            message.setLongProperty(name, Long.parseLong(value));
        } ),
        FLOAT( (message, name, value) -> {
            message.setFloatProperty(name, Float.parseFloat(value));
        } ),
        DOUBLE( (message, name, value) -> {
            message.setDoubleProperty(name, Double.parseDouble(value));
        } ),
        STRING( (message, name, value) -> {
            message.setStringProperty(name, value);
        } );

        private final JmsPropertySetter setter;
        JmsPropertySetterEnum(JmsPropertySetter setter) {
            this.setter = setter;
        }

        @Override
        public void setProperty(Message message, String name, String value) throws JMSException, NumberFormatException {
            setter.setProperty(message, name, value);
        }
    }

    /**
     * This map helps us avoid using JmsPropertySetterEnum.valueOf and dealing with IllegalArgumentException on failed lookup.
     */
    public static Map<String, JmsPropertySetterEnum> propertySetterMap = new HashMap<>();
    static {
        Arrays.stream(JmsPropertySetterEnum.values()).forEach(e -> propertySetterMap.put(e.name().toLowerCase(), e));
    }
}
