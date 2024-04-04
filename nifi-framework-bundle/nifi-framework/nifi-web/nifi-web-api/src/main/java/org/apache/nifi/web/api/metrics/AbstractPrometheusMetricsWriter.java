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
package org.apache.nifi.web.api.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.commons.lang3.StringUtils;
import java.util.Enumeration;
import java.util.regex.Pattern;

public abstract class AbstractPrometheusMetricsWriter implements PrometheusMetricsWriter {
    private final Pattern sampleNamePattern;

    private final Pattern sampleLabelValuePattern;

    private final boolean filteringDisabled;

    public AbstractPrometheusMetricsWriter(
            final String sampleName,
            final String sampleLabelValue
    ) {
        this.sampleNamePattern = StringUtils.isBlank(sampleName) ? null : Pattern.compile(sampleName);
        this.sampleLabelValuePattern = StringUtils.isBlank(sampleLabelValue) ? null : Pattern.compile(sampleLabelValue);
        this.filteringDisabled = StringUtils.isAllBlank(sampleName, sampleLabelValue);
    }

    Enumeration<Collector.MetricFamilySamples> getSamples(final CollectorRegistry registry) {
        final Enumeration<Collector.MetricFamilySamples> samples = registry.metricFamilySamples();
        return filteringDisabled ? samples : new FilteringMetricFamilySamplesEnumeration(
                samples,
                sampleNamePattern,
                sampleLabelValuePattern
        );
    }
}
