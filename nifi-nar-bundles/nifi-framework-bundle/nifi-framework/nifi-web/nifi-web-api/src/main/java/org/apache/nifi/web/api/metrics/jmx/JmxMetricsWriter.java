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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class JmxMetricsWriter {
    private final static ObjectMapper MAPPER = JsonMapper.builder()
            .disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
            .build();
    private final JmxMetricsFilter metricsFilter;

    public JmxMetricsWriter(final JmxMetricsFilter metricsFilter) {
        this.metricsFilter = metricsFilter;
    }

    public void write(final OutputStream outputStream, final Collection<JmxMetricsResult> results) {
        final Collection<JmxMetricsResult> filteredResults = metricsFilter.filter(results);

        try {
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputStream, filteredResults);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
