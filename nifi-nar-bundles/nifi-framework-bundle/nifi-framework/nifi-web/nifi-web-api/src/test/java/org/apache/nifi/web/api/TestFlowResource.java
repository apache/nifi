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
package org.apache.nifi.web.api;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.prometheus.util.JvmMetricsRegistry;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.request.FlowMetricsProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestFlowResource {
    private static final String LABEL_VALUE = TestFlowResource.class.getSimpleName();

    private static final String OTHER_LABEL_VALUE = JmxJvmMetrics.class.getSimpleName();

    private static final String THREAD_COUNT_NAME = "nifi_jvm_thread_count";

    private static final String HEAP_USAGE_NAME = "nifi_jvm_heap_usage";

    private static final String HEAP_USED_NAME = "nifi_jvm_heap_used";

    private static final String HEAP_STARTS_WITH_PATTERN = "nifi_jvm_heap.*";

    private static final String THREAD_COUNT_LABEL = String.format("nifi_jvm_thread_count{instance=\"%s\"", LABEL_VALUE);

    private static final String THREAD_COUNT_OTHER_LABEL = String.format("nifi_jvm_thread_count{instance=\"%s\"", OTHER_LABEL_VALUE);

    @InjectMocks
    private FlowResource resource = new FlowResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    public void testGetFlowMetricsProducerInvalid() {
        assertThrows(ResourceNotFoundException.class, () -> resource.getFlowMetrics(String.class.toString(), Collections.emptySet(), null, null));
    }

    @Test
    public void testGetFlowMetricsPrometheus() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleName() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), THREAD_COUNT_NAME, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertFalse(output.contains(HEAP_USAGE_NAME), "Heap Usage name not filtered");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleNameStartsWithPattern() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), HEAP_STARTS_WITH_PATTERN, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
        assertTrue(output.contains(HEAP_USED_NAME), "Heap Used name not found");
        assertFalse(output.contains(THREAD_COUNT_NAME), "Heap Usage name not filtered");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, LABEL_VALUE);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(LABEL_VALUE), "Label Value not found");
        assertFalse(output.contains(OTHER_LABEL_VALUE), "Other Label Value not filtered");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleNameAndSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), THREAD_COUNT_NAME, LABEL_VALUE);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertTrue(output.contains(THREAD_COUNT_LABEL), "Thread Count with label not found");
        assertTrue(output.contains(THREAD_COUNT_OTHER_LABEL), "Thread Count with other label not found");
        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
    }

    private String getResponseOutput(final Response response) throws IOException {
        final StreamingOutput streamingOutput = (StreamingOutput) response.getEntity();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamingOutput.write(outputStream);
        final byte[] outputBytes = outputStream.toByteArray();
        return new String(outputBytes, StandardCharsets.UTF_8);
    }

    private List<CollectorRegistry> getCollectorRegistries() {
        final JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();
        final CollectorRegistry jvmCollectorRegistry = PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), LABEL_VALUE);
        final CollectorRegistry otherJvmCollectorRegistry = PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), OTHER_LABEL_VALUE);
        return Arrays.asList(jvmCollectorRegistry, otherJvmCollectorRegistry);
    }
}