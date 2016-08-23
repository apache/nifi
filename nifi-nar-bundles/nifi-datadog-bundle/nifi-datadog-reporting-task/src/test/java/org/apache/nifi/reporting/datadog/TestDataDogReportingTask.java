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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.datadog.metrics.MetricsService;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;


public class TestDataDogReportingTask {

    private ProcessGroupStatus status;
    private ProcessorStatus procStatus;
    private ConcurrentHashMap<String, AtomicDouble> metricsMap;
    private MetricRegistry metricRegistry;
    private MetricsService metricsService;
    private String env = "dev";
    private String prefix = "nifi";
    private ReportingContext context;
    private ReportingInitializationContext initContext;
    private ConfigurationContext configurationContext;
    private volatile VirtualMachineMetrics virtualMachineMetrics;
    private Logger logger;

    @Before
    public void setup() {
        initProcessGroupStatus();
        initProcessorStatuses();
        initContexts();
    }

    //init all contexts
    private void initContexts() {
        configurationContext = Mockito.mock(ConfigurationContext.class);
        context = Mockito.mock(ReportingContext.class);
        Mockito.when(context.getProperty(DataDogReportingTask.ENVIRONMENT))
                .thenReturn(new MockPropertyValue(env, null));
        Mockito.when(context.getProperty(DataDogReportingTask.METRICS_PREFIX))
                .thenReturn(new MockPropertyValue(prefix, null));
        Mockito.when(context.getProperty(DataDogReportingTask.API_KEY))
                .thenReturn(new MockPropertyValue("agent", null));
        Mockito.when(context.getProperty(DataDogReportingTask.DATADOG_TRANSPORT))
                .thenReturn(new MockPropertyValue("DataDog Agent", null));
        EventAccess eventAccess = Mockito.mock(EventAccess.class);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(status);
        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);

        logger = Mockito.mock(Logger.class);
        initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        //Mockito.when(initContext.getLogger()).thenReturn(logger);
        metricsMap = new ConcurrentHashMap<>();
        metricRegistry = Mockito.mock(MetricRegistry.class);
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        metricsService = Mockito.mock(MetricsService.class);

    }

    //test onTrigger method
    @Test
    public void testOnTrigger() throws InitializationException, IOException {
        DataDogReportingTask dataDogReportingTask = new TestableDataDogReportingTask();
        dataDogReportingTask.initialize(initContext);
        dataDogReportingTask.setup(configurationContext);
        dataDogReportingTask.onTrigger(context);

        verify(metricsService, atLeast(1)).getProcessorMetrics(Mockito.<ProcessorStatus>any());
        verify(metricsService, atLeast(1)).getJVMMetrics(Mockito.<VirtualMachineMetrics>any());
    }


    //test updating metrics of processors
    @Test
    public void testUpdateMetricsProcessor() throws InitializationException, IOException {
        MetricsService ms = new MetricsService();
        Map<String, Double> processorMetrics = ms.getProcessorMetrics(procStatus);
        Map<String, String> tagsMap = ImmutableMap.of("env", "test");
        DataDogReportingTask dataDogReportingTask = new TestableDataDogReportingTask();
        dataDogReportingTask.initialize(initContext);
        dataDogReportingTask.setup(configurationContext);
        dataDogReportingTask.updateMetrics(processorMetrics, Optional.of("sampleProcessor"), tagsMap);

        verify(metricRegistry).register(eq("nifi.sampleProcessor.FlowFilesReceivedLast5Minutes"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.sampleProcessor.ActiveThreads"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.sampleProcessor.BytesWrittenLast5Minutes"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.sampleProcessor.BytesReadLast5Minutes"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.sampleProcessor.FlowFilesSentLast5Minutes"), Mockito.<Gauge>any());
    }

    //test updating JMV metrics
    @Test
    public void testUpdateMetricsJVM() throws InitializationException, IOException {
        MetricsService ms = new MetricsService();
        Map<String, Double> processorMetrics = ms.getJVMMetrics(virtualMachineMetrics);
        Map<String, String> tagsMap = ImmutableMap.of("env", "test");

        DataDogReportingTask dataDogReportingTask = new TestableDataDogReportingTask();
        dataDogReportingTask.initialize(initContext);
        dataDogReportingTask.setup(configurationContext);

        dataDogReportingTask.updateMetrics(processorMetrics, Optional.<String>absent(), tagsMap);
        verify(metricRegistry).register(eq("nifi.flow.jvm.heap_usage"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.thread_count"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.thread_states.terminated"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.heap_used"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.thread_states.runnable"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.thread_states.timed_waiting"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.uptime"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.daemon_thread_count"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.file_descriptor_usage"), Mockito.<Gauge>any());
        verify(metricRegistry).register(eq("nifi.flow.jvm.thread_states.blocked"), Mockito.<Gauge>any());
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
        protected ConcurrentHashMap<String, AtomicDouble> getMetricsMap() {
            return metricsMap;
        }

    }

}
