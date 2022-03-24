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
import io.prometheus.client.exporter.common.TextFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Enumeration;

/**
 * Prometheus Metrics Writer supporting Prometheus Text Version 0.0.4 with optional filtering
 */
public class TextFormatPrometheusMetricsWriter extends AbstractPrometheusMetricsWriter {

    public TextFormatPrometheusMetricsWriter(final String sampleName, final String sampleLabelValue) {
        super(sampleName, sampleLabelValue);
    }

    @Override
    public void write(final Collection<CollectorRegistry> registries, final OutputStream outputStream) throws IOException {
        try (final Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            for (final CollectorRegistry collectorRegistry : registries) {
                final Enumeration<Collector.MetricFamilySamples> samples = getSamples(collectorRegistry);
                TextFormat.write004(writer, samples);
                writer.flush();
            }
        }
    }
}
