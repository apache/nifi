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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Utility helper class simplify interructions with target AMQP API and NIFI
 * API.
 *
 */
abstract class AMQPUtils {

    public final static String AMQP_PROP_DELIMITER = "$";

    public final static String AMQP_PROP_PREFIX = "amqp" + AMQP_PROP_DELIMITER;

    private final static Logger logger = LoggerFactory.getLogger(AMQPUtils.class);

    private final static List<String> propertyNames;

    static {
        List<String> _propertyNames = new ArrayList<>();
        Method[] methods = BasicProperties.class.getMethods();
        for (Method method : methods) {
            if (Modifier.isPublic(method.getModifiers()) && method.getName().startsWith("get")) {
                _propertyNames.add(AMQP_PROP_PREFIX + method.getName());
            }
        }
        propertyNames = Collections.unmodifiableList(_propertyNames);
    }

    /**
     * Returns a {@link List} of AMQP property names defined in
     * {@link BasicProperties}
     */
    public static List<String> getAmqpPropertyNames() {
        return propertyNames;
    }

    /**
     * Updates {@link FlowFile} with attributes representing AMQP properties
     *
     * @param amqpProperties
     *            instance of {@link BasicProperties}
     * @param flowFile
     *            instance of target {@link FlowFile}
     * @param processSession
     *            instance of {@link ProcessSession}
     */
    public static FlowFile updateFlowFileAttributesWithAmqpProperties(BasicProperties amqpProperties, FlowFile flowFile,
            ProcessSession processSession) {
        try {
            Method[] methods = BasicProperties.class.getMethods();
            for (Method method : methods) {
                if (Modifier.isPublic(method.getModifiers()) && method.getName().startsWith("get")) {
                    Object amqpProperty = method.invoke(amqpProperties);
                    if (amqpProperty != null){
                        flowFile = processSession.putAttribute(flowFile, AMQP_PROP_PREFIX + method.getName(), amqpProperty.toString());
                    }

                }
            }
        } catch (Exception e) {
            logger.warn("Failed to update FlowFile with AMQP attributes", e);
        }
        return flowFile;
    }
}
