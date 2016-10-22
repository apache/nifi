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
package org.apache.nifi.amqp.processors;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Utility helper class simplify interactions with target AMQP API and NIFI API.
 */
abstract class AMQPUtils {

    public final static String AMQP_PROP_DELIMITER = "$";

    public final static String AMQP_PROP_PREFIX = "amqp" + AMQP_PROP_DELIMITER;

    private final static Logger logger = LoggerFactory.getLogger(AMQPUtils.class);

    public enum PropertyNames {
        CONTENT_TYPE(AMQP_PROP_PREFIX + "contentType"),
        CONTENT_ENCODING(AMQP_PROP_PREFIX + "contentEncoding"),
        HEADERS(AMQP_PROP_PREFIX + "headers"),
        DELIVERY_MODE(AMQP_PROP_PREFIX + "deliveryMode"),
        PRIORITY(AMQP_PROP_PREFIX + "priority"),
        CORRELATION_ID(AMQP_PROP_PREFIX + "correlationId"),
        REPLY_TO(AMQP_PROP_PREFIX + "replyTo"),
        EXPIRATION(AMQP_PROP_PREFIX + "expiration"),
        MESSAGE_ID(AMQP_PROP_PREFIX + "messageId"),
        TIMESTAMP(AMQP_PROP_PREFIX + "timestamp"),
        TYPE(AMQP_PROP_PREFIX + "type"),
        USER_ID(AMQP_PROP_PREFIX + "userId"),
        APP_ID(AMQP_PROP_PREFIX + "appId"),
        CLUSTER_ID(AMQP_PROP_PREFIX + "clusterId");

        PropertyNames(String value) {
            this.value = value;
        }

        private final String value;

        private static final Map<String, PropertyNames> lookup = new HashMap<>();

        public static PropertyNames fromValue(String s) {
            return lookup.get(s);
        }

        static {
            for (PropertyNames propertyNames : PropertyNames.values()) {
                lookup.put(propertyNames.getValue(), propertyNames);
            }
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Updates {@link FlowFile} with attributes representing AMQP properties
     *
     * @param amqpProperties instance of {@link BasicProperties}
     * @param flowFile       instance of target {@link FlowFile}
     * @param processSession instance of {@link ProcessSession}
     */
    public static FlowFile updateFlowFileAttributesWithAmqpProperties(BasicProperties amqpProperties, FlowFile flowFile, ProcessSession processSession) {
        if (amqpProperties != null) {
            try {
                Method[] methods = BasicProperties.class.getDeclaredMethods();
                Map<String, String> attributes = new HashMap<>();
                for (Method method : methods) {
                    if (Modifier.isPublic(method.getModifiers()) && method.getName().startsWith("get")) {
                        Object amqpPropertyValue = method.invoke(amqpProperties);
                        if (amqpPropertyValue != null) {
                            String propertyName = extractPropertyNameFromMethod(method);
                            if (isValidAmqpPropertyName(propertyName)) {
                                if (propertyName.equals(PropertyNames.CONTENT_TYPE.getValue())) {
                                    attributes.put(CoreAttributes.MIME_TYPE.key(), amqpPropertyValue.toString());
                                }
                                attributes.put(propertyName, amqpPropertyValue.toString());
                            }
                        }
                    }
                }
                flowFile = processSession.putAllAttributes(flowFile, attributes);
            } catch (Exception e) {
                logger.warn("Failed to update FlowFile with AMQP attributes", e);
            }
        }
        return flowFile;
    }

    /**
     * Will validate if provided name corresponds to valid AMQP property.
     *
     * @param name the name of the property
     * @return 'true' if valid otherwise 'false'
     */
    public static boolean isValidAmqpPropertyName(String name) {
        return PropertyNames.fromValue(name) != null;
    }

    /**
     *
     */
    private static String extractPropertyNameFromMethod(Method method) {
        char c[] = method.getName().substring(3).toCharArray();
        c[0] = Character.toLowerCase(c[0]);
        return AMQP_PROP_PREFIX + new String(c);
    }

    /**
     * Will validate if provided amqpPropValue can be converted to a {@link Map}.
     * Should be passed in the format: amqp$headers=key=value,key=value etc.
     *
     * @param amqpPropValue the value of the property
     * @return {@link Map} if valid otherwise null
     */
    public static Map<String, Object> validateAMQPHeaderProperty(String amqpPropValue) {
        String[] strEntries = amqpPropValue.split(",");
        Map<String, Object> headers = new HashMap<>();
        for (String strEntry : strEntries) {
            String[] kv = strEntry.split("=");
            if (kv.length == 2) {
                headers.put(kv[0].trim(), kv[1].trim());
            } else {
                logger.warn("Malformed key value pair for AMQP header property: " + amqpPropValue);
            }
        }

        return headers;
    }

    /**
     * Will validate if provided amqpPropValue can be converted to an {@link Integer}, and that its
     * value is 1 or 2.
     *
     * @param amqpPropValue the value of the property
     * @return {@link Integer} if valid otherwise null
     */
    public static Integer validateAMQPDeliveryModeProperty(String amqpPropValue) {
        Integer deliveryMode = toInt(amqpPropValue);

        if (deliveryMode == null || !(deliveryMode == 1 || deliveryMode == 2)) {
            logger.warn("Invalid value for AMQP deliveryMode property: " + amqpPropValue);
        }
        return deliveryMode;
    }

    /**
     * Will validate if provided amqpPropValue can be converted to an {@link Integer} and that its
     * value is between 0 and 9 (inclusive).
     *
     * @param amqpPropValue the value of the property
     * @return {@link Integer} if valid otherwise null
     */
    public static Integer validateAMQPPriorityProperty(String amqpPropValue) {
        Integer priority = toInt(amqpPropValue);

        if (priority == null || !(priority >= 0 && priority <= 9)) {
            logger.warn("Invalid value for AMQP priority property: " + amqpPropValue);
        }
        return priority;
    }

    /**
     * Will validate if provided amqpPropValue can be converted to a {@link Date}.
     *
     * @param amqpPropValue the value of the property
     * @return {@link Date} if valid otherwise null
     */
    public static Date validateAMQPTimestampProperty(String amqpPropValue) {
        Long timestamp = toLong(amqpPropValue);

        if (timestamp == null) {
            logger.warn("Invalid value for AMQP timestamp property: " + amqpPropValue);
            return null;
        }

        //milliseconds are lost when sending to AMQP
        return new Date(timestamp);
    }

    /**
     * Takes a {@link String} and tries to convert to an {@link Integer}.
     *
     * @param strVal the value to be converted
     * @return {@link Integer} if valid otherwise null
     */
    private static Integer toInt(String strVal) {
        try {
            return Integer.parseInt(strVal);
        } catch (NumberFormatException aE) {
            return null;
        }
    }

    /**
     * Takes a {@link String} and tries to convert to a {@link Long}.
     *
     * @param strVal the value to be converted
     * @return {@link Long} if valid otherwise null
     */
    private static Long toLong(String strVal) {
        try {
            return Long.parseLong(strVal);
        } catch (NumberFormatException aE) {
            return null;
        }
    }
}
