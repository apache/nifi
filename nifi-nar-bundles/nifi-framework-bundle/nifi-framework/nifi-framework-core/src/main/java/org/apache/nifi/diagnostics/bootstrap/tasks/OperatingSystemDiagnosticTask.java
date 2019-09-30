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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class OperatingSystemDiagnosticTask implements DiagnosticTask {
    private static final Logger logger = LoggerFactory.getLogger(OperatingSystemDiagnosticTask.class);
    private static final Set<String> IGNORABLE_ATTRIBUTE_NAMES = new HashSet<>(Arrays.asList("ObjectName", ""));

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        final List<String> details = new ArrayList<>();

        final NumberFormat numberFormat = NumberFormat.getInstance();

        try {
            final SortedMap<String, String> attributes = new TreeMap<>();

            final ObjectName osObjectName = os.getObjectName();
            final MBeanInfo mbeanInfo = ManagementFactory.getPlatformMBeanServer().getMBeanInfo(osObjectName);
            for (final MBeanAttributeInfo attributeInfo : mbeanInfo.getAttributes()) {
                final String attributeName = attributeInfo.getName();
                if (IGNORABLE_ATTRIBUTE_NAMES.contains(attributeName)) {
                    continue;
                }

                final Object attributeValue = ManagementFactory.getPlatformMBeanServer().getAttribute(osObjectName, attributeName);

                if (attributeValue instanceof Number) {
                    attributes.put(attributeName, numberFormat.format(attributeValue));
                } else {
                    attributes.put(attributeName, String.valueOf(attributeValue));
                }
            }

            attributes.forEach((key, value) -> details.add(key + " : " + value));
        } catch (final Exception e) {
            logger.error("Failed to obtain Operating System details", e);
            return new StandardDiagnosticsDumpElement("Operating System / Hardware", Collections.singletonList("Failed to obtain Operating System details"));
        }

        return new StandardDiagnosticsDumpElement("Operating System / Hardware", details);
    }
}
