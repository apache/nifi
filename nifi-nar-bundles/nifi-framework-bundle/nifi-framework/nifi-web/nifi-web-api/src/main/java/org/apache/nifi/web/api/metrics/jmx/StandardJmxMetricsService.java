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

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.JmxMetricsResultDTO;

import java.util.Collection;

public class StandardJmxMetricsService implements JmxMetricsService {
    private NiFiProperties properties;
    private JmxMetricsCollector metricsCollector;

    @Override
    public Collection<JmxMetricsResultDTO> getFilteredMBeanMetrics(final String beanNameFilter) {
        final String allowedFilterPattern = properties.getProperty(NiFiProperties.WEB_JMX_METRICS_ALLOWED_FILTER_PATTERN);
        final JmxMetricsFilter metricsFilter = new JmxMetricsFilter(allowedFilterPattern, beanNameFilter);
        return metricsFilter.filter(metricsCollector.getBeanMetrics());
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    public void setMetricsCollector(final JmxMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
}
