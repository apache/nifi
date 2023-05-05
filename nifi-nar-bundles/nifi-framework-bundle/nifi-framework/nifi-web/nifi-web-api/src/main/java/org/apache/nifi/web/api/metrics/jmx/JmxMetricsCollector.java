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
package org.apache.nifi.web.api.metrics.jmx;

import org.apache.nifi.web.api.dto.JmxMetricsResultDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class JmxMetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricsCollector.class);
    private final static String PATTERN_FOR_ALL_OBJECT_NAMES = "*:*";
    private final JmxMetricsResultConverter resultConverter;

    public JmxMetricsCollector(final JmxMetricsResultConverter metricsResultConverter) {
        this.resultConverter = metricsResultConverter;
    }

    public Collection<JmxMetricsResultDTO> getBeanMetrics() {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Set<ObjectInstance> instances;
        try {
            instances = mBeanServer.queryMBeans(new ObjectName(PATTERN_FOR_ALL_OBJECT_NAMES), null);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Invalid ObjectName pattern", e);
        }

        final Collection<JmxMetricsResultDTO> results = new ArrayList<>();
        for (final ObjectInstance instance : instances) {
            final MBeanInfo info;
            try {
                info = mBeanServer.getMBeanInfo(instance.getObjectName());
            } catch (InstanceNotFoundException | ReflectionException | IntrospectionException e) {
                continue;
            }

            for (MBeanAttributeInfo attribute : info.getAttributes()) {
                try {
                    final String beanName = instance.getObjectName().getCanonicalName();
                    final String attributeName = attribute.getName();
                    final Object attributeValue = resultConverter.convert(mBeanServer.getAttribute(instance.getObjectName(), attribute.getName()));

                    results.add(new JmxMetricsResultDTO(beanName, attributeName, attributeValue));
                } catch (final MBeanException | RuntimeMBeanException | ReflectionException | InstanceNotFoundException | AttributeNotFoundException e) {
                    //Empty or invalid attributes should not stop the loop.
                    LOGGER.debug("MBean Object [{}] invalid attribute [{}] found", instance.getObjectName(), attribute.getName());
                }
            }
        }
        return results;
    }
}
