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
package org.apache.nifi.reporting.datadog;

import com.codahale.metrics.MetricRegistry;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.datadog.metrics.MetricsService;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class TestDataDogReportingTask {

    private ProcessGroupStatus status;
    private ProcessorStatus procStatus;
    private ConcurrentHashMap<String, Double> metricsMap;
    private MetricRegistry metricRegistry;
    private MetricsService metricsService;
    private ReportingContext context;
    private ReportingInitializationContext initContext;
    private ConfigurationContext configurationContext;
    private volatile JmxJvmMetrics virtualMachineMetrics;

    @BeforeEach
    public void setup() {
        initProcessGroupStatus();
        initProcessorStatuses();
        initContexts();
    }

    private void initContexts() {
        configurationContext = mock(ConfigurationContext.class);
        context = mock(ReportingContext.class);
        when(context.getProperty(DataDogReportingTask.ENVIRONMENT))
                .thenReturn(new MockPropertyValue("dev", null));
        when(context.getProperty(DataDogReportingTask.METRICS_PREFIX))
                .thenReturn(new MockPropertyValue("nifi", null));
        when(context.getProperty(DataDogReportingTask.API_KEY))
                .thenReturn(new MockPropertyValue("agent", null));
        when(context.getProperty(DataDogReportingTask.DATADOG_TRANSPORT))
                .thenReturn(new MockPropertyValue("DataDog Agent", null));
        EventAccess eventAccess = mock(EventAccess.class);
        when(eventAccess.getControllerStatus()).thenReturn(status);
        when(context.getEventAccess()).thenReturn(eventAccess);

        initContext = mock(ReportingInitializationContext.class);
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(initContext.getLogger()).thenReturn(new MockComponentLogger());
        metricsMap = new ConcurrentHashMap<>();
        metricRegistry = mock(MetricRegistry.class);
        virtualMachineMetrics = JmxJvmMetrics.getInstance();
        metricsService = mock(MetricsService.class);
    }

    @Test
    public void testOnTrigger() throws InitializationException {
        DataDogReportingTask dataDogReportingTask = new TestableDataDogReportingTask();
        dataDogReportingTask.initialize(initContext);
        dataDogReportingTask.setup(configurationContext);
        dataDogReportingTask.onTrigger(context);

        verify(metricsService, atLeast(1)).getProcessorMetrics(any());
        verify(metricsService, atLeast(1)).getJVMMetrics(any());
    }

    @Test
    public void testUpdateMetricsProcessor() throws InitializationException {
        MetricsService ms = new MetricsService();
        Map<String, Double> processorMetrics = ms.getProcessorMetrics(procStatus);
        Map<String, String> tagsMap = Collections.singletonMap("env", "test");
        DataDogReportingTask dataDogReportingTask = new TestableDataDogReportingTask();
        dataDogReportingTask.initialize(initContext);
        dataDogReportingTask.setup(configurationContext);
        dataDogReportingTask.updateMetrics(processorMetrics, tagsMap);

        verify(metricRegistry).register(eq("nifi.FlowFilesReceivedLast5Minutes"), any());
        verify(metricRegistry).register(eq("nifi.ActiveThreads"), any());
        verify(metricRegistry).register(eq("nifi.BytesWrittenLast5Minutes"), any());
        verify(metricRegistry).register(eq("nifi.BytesReadLast5Minutes"), any());
        verify(metricRegistry).register(eq("nifi.FlowFilesSentLast5Minutes"), any());
    }

    @Test
    public void testUpdateMetricsJVM() throws InitializationException {
        MetricsService ms = new MetricsService();
        Map<String, Double> processorMetrics = ms.getJVMMetrics(virtualMachineMetrics);
        Map<String, String> tagsMap = Collections.singletonMap("env", "test");

        DataDogReportingTask dataDogReportingTask = new TestableDataDogReportingTask();
        dataDogReportingTask.initialize(initContext);
        dataDogReportingTask.setup(configurationContext);

        dataDogReportingTask.updateMetrics(processorMetrics, tagsMap);
        verify(metricRegistry).register(eq("nifi.jvm.heap_usage"), any());
        verify(metricRegistry).register(eq("nifi.jvm.thread_count"), any());
        verify(metricRegistry).register(eq("nifi.jvm.thread_states.terminated"), any());
        verify(metricRegistry).register(eq("nifi.jvm.heap_used"), any());
        verify(metricRegistry).register(eq("nifi.jvm.thread_states.runnable"), any());
        verify(metricRegistry).register(eq("nifi.jvm.thread_states.timed_waiting"), any());
        verify(metricRegistry).register(eq("nifi.jvm.uptime"), any());
        verify(metricRegistry).register(eq("nifi.jvm.daemon_thread_count"), any());
        verify(metricRegistry).register(eq("nifi.jvm.file_descriptor_usage"), any());
        verify(metricRegistry).register(eq("nifi.jvm.thread_states.blocked"), any());
    }

    private void initProcessGroupStatus() {
        status = new ProcessGroupStatus();
        status.setId("1234");
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesRead(60000L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);
        status.setInputCount(2);
        status.setOutputCount(4);
    }

    private void initProcessorStatuses() {
        procStatus = new ProcessorStatus();
        procStatus.setProcessingNanos(123456789);
        procStatus.setInputCount(2);
        procStatus.setOutputCount(4);
        procStatus.setActiveThreadCount(6);
        procStatus.setBytesSent(1256);
        procStatus.setName("sampleProcessor");
        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        ProcessGroupStatus groupStatus = new ProcessGroupStatus();
        groupStatus.setProcessorStatus(processorStatuses);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus);
        status.setProcessGroupStatus(groupStatuses);
    }

    private class TestableDataDogReportingTask extends DataDogReportingTask {
        @Override
        protected MetricsService getMetricsService() {
            return metricsService;
        }

        @Override
        protected DDMetricRegistryBuilder getMetricRegistryBuilder() {
            return new DDMetricRegistryBuilder();
        }

        @Override
        protected MetricRegistry getMetricRegistry() {
            return metricRegistry;
        }

        @Override
        protected ConcurrentHashMap<String, Double> getMetricsMap() {
            return metricsMap;
        }

    }
}
