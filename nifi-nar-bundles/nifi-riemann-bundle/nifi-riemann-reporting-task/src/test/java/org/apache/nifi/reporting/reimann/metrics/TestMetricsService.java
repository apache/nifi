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
package org.apache.nifi.reporting.reimann.metrics;

import java.util.List;
import java.util.Map;

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.MockBulletin;
import org.apache.nifi.reporting.riemann.RiemannTestSuite;
import org.apache.nifi.reporting.riemann.attributes.AttributeNames;
import org.apache.nifi.reporting.riemann.metrics.MetricNames;
import org.apache.nifi.reporting.riemann.metrics.MetricsService;
import org.junit.Assert;
import org.junit.Test;

import com.aphyr.riemann.Proto;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.VirtualMachineMetrics;

public class TestMetricsService extends RiemannTestSuite {

    protected Map<String, Proto.Event> buildEventsMap(List<Proto.Event> events) {
        Map<String, Proto.Event> eventsMap = Maps.newHashMapWithExpectedSize(events.size());
        for (Proto.Event event : events) {
            eventsMap.put(event.getService(), event);
        }
        return eventsMap;
    }

    protected String buildProcessGroupServiceName(String metricName) {
        return MetricsService.SEP.join(SERVICE_PREFIX, NAME, metricName, MetricNames.SERVICE_SUFFIX);
    }

    protected String buildProcessorServiceName(String metricName) {
        return MetricsService.SEP.join(SERVICE_PREFIX, PROCESSOR_TYPE, PROCESSOR_ID, metricName, MetricNames.SERVICE_SUFFIX);
    }

    @Test
    public void testGetProcessGroupStatusMetrics() {
        final List<Proto.Event> processorGroupEvents = service.createProcessGroupEvents(processGroupStatus);
        final Map<String, Proto.Event> processGroupEventsMap = buildEventsMap(processorGroupEvents);

        for (String metricName : MetricNames.PROCESS_GROUP_METRICS) {
            String serviceName = buildProcessGroupServiceName(metricName);
            Assert.assertTrue(serviceName + " is missing from returned events", processGroupEventsMap.containsKey(serviceName));
        }

        Assert.assertEquals(FLOW_FILES_RECEIVED, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.FLOW_FILES_RECEIVED)).getMetricSint64());
        Assert.assertEquals(FLOW_FILES_SENT, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.FLOW_FILES_SENT)).getMetricSint64());
        Assert.assertEquals(BYTES_TRANSFERRED, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.BYTES_TRANSFERRED)).getMetricSint64());
        Assert.assertEquals(BYTES_READ, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.BYTES_READ)).getMetricSint64());
        Assert.assertEquals(BYTES_SENT, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.BYTES_SENT)).getMetricSint64());
        Assert.assertEquals(QUEUED_COUNT, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.FLOW_FILES_QUEUED)).getMetricSint64());
        Assert.assertEquals(QUEUED_CONTENT_SIZE, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.BYTES_QUEUED)).getMetricSint64());
        Assert.assertEquals(BYTES_WRITTEN, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.BYTES_WRITTEN)).getMetricSint64());
        Assert.assertEquals(ACTIVE_THREADS, processGroupEventsMap.get(buildProcessGroupServiceName(MetricNames.ACTIVE_THREADS)).getMetricSint64());
    }

    @Test
    public void testGetProcessorStatusMetrics() {
        final List<Proto.Event> processorGroupEvents = service.createProcessorEvents(Lists.newArrayList(processorStatus));
        final Map<String, Proto.Event> processorEventsMap = buildEventsMap(processorGroupEvents);

        for (String metricName : MetricNames.PROCESSOR_METRICS) {
            String serviceName = buildProcessorServiceName(metricName);
            Assert.assertTrue(serviceName + " is missing from returned events", processorEventsMap.containsKey(serviceName));
        }

        Assert.assertEquals(FLOW_FILES_RECEIVED, processorEventsMap.get(buildProcessorServiceName(MetricNames.FLOW_FILES_RECEIVED)).getMetricSint64());
        Assert.assertEquals(FLOW_FILES_SENT, processorEventsMap.get(buildProcessorServiceName(MetricNames.FLOW_FILES_SENT)).getMetricSint64());
        Assert.assertEquals(FLOW_FILES_REMOVED, processorEventsMap.get(buildProcessorServiceName(MetricNames.FLOW_FILES_REMOVED)).getMetricSint64());
        Assert.assertEquals(BYTES_READ, processorEventsMap.get(buildProcessorServiceName(MetricNames.BYTES_READ)).getMetricSint64());
        Assert.assertEquals(BYTES_SENT, processorEventsMap.get(buildProcessorServiceName(MetricNames.BYTES_SENT)).getMetricSint64());
        Assert.assertEquals(BYTES_RECEIVED, processorEventsMap.get(buildProcessorServiceName(MetricNames.BYTES_RECEIVED)).getMetricSint64());
        Assert.assertEquals(BYTES_WRITTEN, processorEventsMap.get(buildProcessorServiceName(MetricNames.BYTES_WRITTEN)).getMetricSint64());
        Assert.assertEquals(ACTIVE_THREADS, processorEventsMap.get(buildProcessorServiceName(MetricNames.ACTIVE_THREADS)).getMetricSint64());
        Assert.assertEquals(AVERAGE_LINEAGE_DURATION, processorEventsMap.get(buildProcessorServiceName(MetricNames.AVERAGE_LINEAGE_DURATION)).getMetricSint64());
    }

    @Test
    public void testGetVirtualMachineMetrics() {
        final VirtualMachineMetrics virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        final List<Proto.Event> jvmEvents = service.createJVMEvents(virtualMachineMetrics);
        final Map<String, Proto.Event> jvmEventsMap = buildEventsMap(jvmEvents);

        for (String metricName : MetricNames.JVM_METRICS) {
            String serviceName = MetricsService.SEP.join(service.getServicePrefix(), metricName);
            Assert.assertTrue(serviceName + " is missing from returned events", jvmEventsMap.containsKey(serviceName));
        }
    }

    @Test
    public void testBulletinRepositoryEvent() {
        String level = "ERROR";
        String message = "Test Message";
        String category = "test-category";

        Bulletin bulletin = new MockBulletin(System.currentTimeMillis());
        bulletin.setGroupId(PROCESSOR_GROUP_ID);
        bulletin.setLevel(level);
        bulletin.setMessage(message);
        bulletin.setCategory(category);
        bulletin.setNodeAddress(HOST);
        bulletin.setSourceId(PROCESSOR_ID);
        bulletin.setSourceName("SomeSource");
        bulletin.setSourceType(ComponentType.PROCESSOR);
        List<Bulletin> bulletins = Lists.newArrayList(bulletin);
        Proto.Event event = service.createBulletinEvents(bulletins).get(0);

        Assert.assertEquals(level, event.getState());
        Assert.assertEquals(level + " : " + message, event.getDescription());
        Assert.assertEquals(HOST, event.getHost());
        Assert.assertTrue(event.getService().contains(PROCESSOR_ID));

        Map<String, String> attributesMap = buildAttributesMap(event.getAttributesList());
        Assert.assertEquals(PROCESSOR_ID, attributesMap.get(AttributeNames.PROCESSOR_ID));
        Assert.assertEquals(PROCESSOR_GROUP_ID, attributesMap.get(AttributeNames.PROCESSOR_GROUP_ID));
        Assert.assertEquals(category, attributesMap.get(AttributeNames.CATEGORY));
    }

    protected Map<String, String> buildAttributesMap(List<Proto.Attribute> attributes) {
        Map<String, String> attributesMap = Maps.newHashMapWithExpectedSize(attributes.size());
        for (Proto.Attribute attribute : attributes) {
            attributesMap.put(attribute.getKey(), attribute.getValue());
        }
        return attributesMap;
    }
}
