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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.prometheus.util.BulletinMetricsRegistry;
import org.apache.nifi.prometheus.util.ClusterMetricsRegistry;
import org.apache.nifi.prometheus.util.ConnectionAnalyticsMetricsRegistry;
import org.apache.nifi.prometheus.util.JvmMetricsRegistry;
import org.apache.nifi.prometheus.util.NiFiMetricsRegistry;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.request.FlowMetricsProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private static final String ROOT_FIELD_NAME = "beans";
    private static final String SAMPLE_NAME_JVM = ".*jvm.*";
    private static final String SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP = "RootProcessGroup";
    private static final String SAMPLE_LABEL_VALUES_PROCESS_GROUP = "ProcessGroup";
    private static final String COMPONENT_TYPE_LABEL = "component_type";
    private static final int COMPONENT_TYPE_VALUE_INDEX = 1;
    private static final String CLUSTER_TYPE_LABEL = "cluster";
    private static final String CLUSTER_LABEL_KEY = "instance";

    @InjectMocks
    private FlowResource resource = new FlowResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    public void testGetFlowMetricsProducerInvalid() {
        assertThrows(ResourceNotFoundException.class, () -> resource.getFlowMetrics(String.class.toString(), Collections.emptySet(), null, null, null));
    }

    @Test
    public void testGetFlowMetricsPrometheus() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, null, null);

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

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), THREAD_COUNT_NAME, null, null);

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

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), HEAP_STARTS_WITH_PATTERN, null, null);

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

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, LABEL_VALUE, null);

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

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), THREAD_COUNT_NAME, LABEL_VALUE, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertTrue(output.contains(THREAD_COUNT_LABEL), "Thread Count with label not found");
        assertTrue(output.contains(THREAD_COUNT_OTHER_LABEL), "Thread Count with other label not found");
        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJson() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), null, null, ROOT_FIELD_NAME);

        assertNotNull(response);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(13, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(3L, result.get(SAMPLE_NAME_JVM));
        assertEquals(4L, result.get(SAMPLE_LABEL_VALUES_PROCESS_GROUP));
        assertEquals(2L, result.get(SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP));
        assertEquals(4L, result.get(CLUSTER_LABEL_KEY));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleName() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), SAMPLE_NAME_JVM, null, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(3, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(3L, result.get(SAMPLE_NAME_JVM));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleNameStartsWithPattern() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), HEAP_STARTS_WITH_PATTERN, null, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(2, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(2L, result.get(SAMPLE_NAME_JVM));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), null, SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(2, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(2L, result.get(SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleNameAndSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), SAMPLE_NAME_JVM, SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(5, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(3L, result.get(SAMPLE_NAME_JVM));
        assertEquals(2L, result.get(SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP));
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

    private Map<String, List<Sample>> convertJsonResponseToMap(final Response response) throws IOException {
        final TypeReference<HashMap<String, List<Sample>>> typeReference = new TypeReference<HashMap<String, List<Sample>>>() {};
        final ObjectMapper mapper = new ObjectMapper();
        final SimpleModule module = new SimpleModule();

        module.addDeserializer(Sample.class, new SampleDeserializer());
        mapper.registerModule(module);

        final String json = getResponseOutput(response);
        return mapper.readValue(json, typeReference);
    }

    private Map<String, Long> getResult(final List<Sample> registries) {
        return registries.stream()
                .collect(Collectors.groupingBy(
                        sample -> getResultKey(sample),
                        Collectors.counting()));
    }

    private String getResultKey(final Sample sample) {
        if (sample.labelNames.contains(COMPONENT_TYPE_LABEL)) {
            return sample.labelValues.get(COMPONENT_TYPE_VALUE_INDEX);
        }
        if (sample.name.startsWith(CLUSTER_TYPE_LABEL)) {
            return CLUSTER_LABEL_KEY;
        }
        return SAMPLE_NAME_JVM;
    }

    private static List<CollectorRegistry> getCollectorRegistriesForJson() {
        final List<CollectorRegistry> registryList = new ArrayList<>();

        registryList.add(getNifiMetricsRegistry());
        registryList.add(getConnectionMetricsRegistry());
        registryList.add(getJvmMetricsRegistry());
        registryList.add(getBulletinMetricsRegistry());
        registryList.add(getClusterMetricsRegistry());

        return registryList;

    }

    private static CollectorRegistry getNifiMetricsRegistry() {
        final NiFiMetricsRegistry registry = new NiFiMetricsRegistry();

        registry.setDataPoint(136, "TOTAL_BYTES_READ",
                "RootPGId", SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, "rootPGName", "", "");
        registry.setDataPoint(136, "TOTAL_BYTES_READ",
                "PGId", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "PGName", "RootPGId", "");

        return registry.getRegistry();
    }

    private static CollectorRegistry getConnectionMetricsRegistry() {
        final ConnectionAnalyticsMetricsRegistry connectionMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();

        connectionMetricsRegistry.setDataPoint(1.0,
                "TIME_TO_BYTES_BACKPRESSURE_PREDICTION",
                "PGId", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "success", "connComponentId",
                "RootPGId", "sourceId", "sourceName", "destinationId", "destinationName");
        connectionMetricsRegistry.setDataPoint(1.0,
                "TIME_TO_BYTES_BACKPRESSURE_PREDICTION",
                "RootPGId", SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, "rootPGName", "", "", "", "", "", "");

        return connectionMetricsRegistry.getRegistry();
    }

    private static CollectorRegistry getJvmMetricsRegistry() {
        final JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();

        jvmMetricsRegistry.setDataPoint(4.0, "JVM_HEAP_USED", "instanceId");
        jvmMetricsRegistry.setDataPoint(6.0, "JVM_HEAP_USAGE", "instanceId");
        jvmMetricsRegistry.setDataPoint(10.0, "JVM_THREAD_COUNT", "instanceId");

        return jvmMetricsRegistry.getRegistry();
    }

    private static CollectorRegistry getBulletinMetricsRegistry() {
        final BulletinMetricsRegistry bulletinMetricsRegistry = new BulletinMetricsRegistry();

        bulletinMetricsRegistry.setDataPoint(1, "BULLETIN", "B1Id", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "PGId", "RootPGId",
                "nodeAddress", "category", "sourceName", "sourceId", "level");
        bulletinMetricsRegistry.setDataPoint(1, "BULLETIN", "B2Id", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "PGId", "RootPGId",
                "nodeAddress", "category", "sourceName", "sourceId", "level");

        return bulletinMetricsRegistry.getRegistry();
    }

    private static CollectorRegistry getClusterMetricsRegistry() {
        final ClusterMetricsRegistry clusterMetricsRegistry = new ClusterMetricsRegistry();

        clusterMetricsRegistry.setDataPoint(1, "IS_CLUSTERED", "B1Id");
        clusterMetricsRegistry.setDataPoint(1, "IS_CONNECTED_TO_CLUSTER", "B1Id");
        clusterMetricsRegistry.setDataPoint(2, "CONNECTED_NODE_COUNT", "B1Id", "2 / 3");
        clusterMetricsRegistry.setDataPoint(3, "TOTAL_NODE_COUNT", "B1Id");

        return clusterMetricsRegistry.getRegistry();
    }


    private static class SampleDeserializer extends StdDeserializer<Sample> {
        protected SampleDeserializer() {
            super(Sample.class);
        }

        @Override
        public Sample deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            final JsonNode node = jsonParser.getCodec().readTree(jsonParser);

            final String name = node.get("name").asText();
            final List<String> labelNames = new ArrayList<>();
            node.get("labelNames").elements().forEachRemaining(e -> labelNames.add(e.asText()));
            final List<String> labelValues = new ArrayList<>();
            node.get("labelValues").elements().forEachRemaining(e -> labelValues.add(e.asText()));
            final double value = node.get("value").asDouble();

            return new Sample(name, labelNames, labelValues, value);
        }
    }
}