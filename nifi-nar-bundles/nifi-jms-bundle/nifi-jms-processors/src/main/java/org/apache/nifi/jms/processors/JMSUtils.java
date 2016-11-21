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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.JmsHeaders;

/**
 *
 *
 */
class JMSUtils {

    private final static Logger logger = LoggerFactory.getLogger(JMSUtils.class);

    public static Map<String, Object> extractMessageHeaders(Message message) {
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

            String replyToDestinationName = retrieveDestinationName(message.getJMSReplyTo(), JmsHeaders.REPLY_TO);
            if (replyToDestinationName != null) {
                messageHeaders.put(JmsHeaders.REPLY_TO, replyToDestinationName);
            }
            String destinationName = retrieveDestinationName(message.getJMSDestination(), JmsHeaders.DESTINATION);
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
     * @param message
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> extractMessageProperties(Message message) {
        Map<String, String> properties = new HashMap<>();
        try {
            Enumeration<String> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement();
                properties.put(propertyName, String.valueOf(message.getObjectProperty(propertyName)));
            }
        } catch (JMSException e) {
            logger.warn("Failed to extract message properties", e);
        }
        return properties;
    }

    /**
     *
     * @param destination
     * @param headerName
     * @return
     */
    private static String retrieveDestinationName(Destination destination, String headerName) {
       String destinationName = null;
       if (destination != null) {
           try {
               destinationName = (destination instanceof Queue) ? ((Queue) destination).getQueueName()
                       : ((Topic) destination).getTopicName();
           } catch (JMSException e) {
                logger.warn("Failed to extract message headers", e);
           }
       }
       return destinationName;
   }
}
