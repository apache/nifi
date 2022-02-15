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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Enumeration;

/**
 * Prometheus Metrics Writer with Json output format and optional filtering
 */
public class JsonFormatPrometheusMetricsWriter extends AbstractPrometheusMetricsWriter {
    private final String rootFieldName;

    public JsonFormatPrometheusMetricsWriter(final String sampleName, final String sampleLabelValue, final String rootFieldName) {
        super(sampleName, sampleLabelValue);
        this.rootFieldName = rootFieldName == null ? "samples" : rootFieldName;
    }

    @Override
    public void write(final Collection<CollectorRegistry> registries, final OutputStream outputStream) throws IOException {
        JsonFactory factory = new JsonFactory();
        try (final Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        final JsonGenerator generator = factory.createGenerator(writer)) {
            generator.setCodec(new ObjectMapper());
            generator.writeStartObject();
            generator.writeFieldName(rootFieldName);
            generator.writeStartArray();
            for (final CollectorRegistry collectorRegistry : registries) {
                final Enumeration<Collector.MetricFamilySamples> samples = getSamples(collectorRegistry);
                while (samples.hasMoreElements()) {
                    final Collector.MetricFamilySamples samples2 = samples.nextElement();
                    for (Collector.MetricFamilySamples.Sample sample : samples2.samples) {
                        generator.writeObject(sample);
                        generator.flush();
                    }
                    generator.flush();
                }
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.flush();
        }
    }
}
